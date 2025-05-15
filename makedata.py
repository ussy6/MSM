import netCDF4 as nc
import pandas as pd
import numpy as np
import os
import subprocess
from datetime import datetime, timedelta
import json
import argparse
import shutil

def calculate_storage_requirements(start_date, end_date, file_size_mb=139.9, targets=None):
    """計画されたダウンロードに必要なストレージ容量を計算して表示する"""
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # 日数の計算
    days = (end_dt - start_dt).days + 1
    
    # 合計必要ストレージ
    total_size_mb = days * file_size_mb
    total_size_gb = total_size_mb / 1024
    
    # 地点数が多い場合のCSV出力サイズを概算（netCDFからの抽出なのでかなり小さくなる）
    csv_size_estimate = 0
    if targets:
        # 1地点あたり約0.5MB/日と仮定
        csv_size_estimate = days * len(targets) * 0.5
    
    print(f"ストレージ必要量の概算:")
    print(f"- ダウンロード期間: {start_date} から {end_date} ({days}日間)")
    print(f"- netCDFファイル: 約 {total_size_mb:.1f} MB ({total_size_gb:.2f} GB)")
    
    if csv_size_estimate > 0:
        print(f"- CSV出力ファイル: 約 {csv_size_estimate:.1f} MB (処理する地点数: {len(targets)})")
    
    print(f"- 合計必要容量: 約 {(total_size_mb + csv_size_estimate):.1f} MB ({(total_size_gb + csv_size_estimate/1024):.2f} GB)")
    
    # 空き容量の確認
    try:
        total, used, free = shutil.disk_usage('/')
        free_gb = free / (1024 ** 3)
        print(f"- 現在の空き容量: {free_gb:.2f} GB")
        
        if free_gb < (total_size_gb + csv_size_estimate/1024):
            print("警告: 必要なストレージが現在の空き容量を超えています。")
    except:
        print("注: 空き容量の確認に失敗しました。十分な空き容量があることを確認してください。")
    
    return total_size_mb

def download_msm_data(start_date, end_date, save_dir, skip_existing=True):
    """MSMデータをダウンロードする。すでに存在するファイルはスキップできる"""
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    current_dt = start_dt
    
    downloaded_count = 0
    skipped_count = 0
    failed_count = 0
    
    print(f"\nMSMデータのダウンロードを開始します ({start_date} から {end_date})...")

    while current_dt <= end_dt:
        year = current_dt.strftime('%Y')
        month_day = current_dt.strftime('%m%d')
        year_dir = os.path.join(save_dir, year)
        if not os.path.exists(year_dir):
            os.makedirs(year_dir)
        
        url = f"http://database.rish.kyoto-u.ac.jp/arch/jmadata/data/gpv/netcdf/MSM-S/{year}/{month_day}.nc"
        output_path = os.path.join(year_dir, f"{month_day}.nc")

        # 既存ファイルのチェック
        if skip_existing and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            print(f"既存ファイルをスキップ: {output_path}")
            skipped_count += 1
        else:
            try:
                print(f"ダウンロード中: {url}")
                subprocess.run(["curl", "-o", output_path, url], check=True)
                downloaded_count += 1
            except subprocess.CalledProcessError:
                print(f"エラー: ファイルのダウンロードに失敗しました: {url}")
                failed_count += 1

        current_dt += timedelta(days=1)
    
    print(f"\nダウンロード完了:")
    print(f"- ダウンロード成功: {downloaded_count} ファイル")
    print(f"- 既存ファイルスキップ: {skipped_count} ファイル")
    if failed_count > 0:
        print(f"- ダウンロード失敗: {failed_count} ファイル")

def extract_msm_data_to_csv(nc_file, targets, output_dir):
    """複数の地点でのMSMデータをnetCDFファイルから抽出し、CSVファイルに保存する"""
    try:
        dataset = nc.Dataset(nc_file)
        
        lats = dataset.variables['lat'][:]
        lons = dataset.variables['lon'][:]
        
        # ファイル名の準備（日付部分を抽出）
        file_basename = os.path.basename(nc_file)
        date_str = os.path.splitext(file_basename)[0]  # 例：0501
        
        # 親ディレクトリから年を取得
        nc_dir = os.path.dirname(nc_file)
        year_str = os.path.basename(nc_dir)  # 例：2020
        
        # 完全な日付文字列を作成（YYYYMMDD形式）
        full_date_str = f"{year_str}{date_str}"  # 例：20200501
        
        # 各ターゲット地点に対してデータを抽出
        for target_name, target_info in targets.items():
            target_lat = target_info['latitude']
            target_lon = target_info['longitude']
            
            # 最も近いグリッドポイントのインデックスを見つける
            lat_idx = np.abs(lats - target_lat).argmin()
            lon_idx = np.abs(lons - target_lon).argmin()
            
            # 実際のグリッドポイントの座標
            actual_lat = float(lats[lat_idx])
            actual_lon = float(lons[lon_idx])
            
            data = {'time': [], 'grid_latitude': [], 'grid_longitude': []}
            
            times = dataset.variables['time'][:]
            time_values = nc.num2date(times, units=dataset.variables['time'].units)
            data['time'] = time_values
            
            # 各時間ポイントに対してグリッド座標を追加
            for _ in range(len(time_values)):
                data['grid_latitude'].append(actual_lat)
                data['grid_longitude'].append(actual_lon)
            
            variables_of_interest = ['psea', 'sp', 'u', 'v', 'temp', 'rh', 'r1h', 'ncld', 'dswrf']
            for var_name in variables_of_interest:
                try:
                    var = dataset.variables[var_name]
                    values = var[:, lat_idx, lon_idx]
                    scale_factor = getattr(var, 'scale_factor', 1.0)
                    add_offset = getattr(var, 'add_offset', 0.0)
                    # corrected_values = values * scale_factor + add_offset # TODO 補正をするとおかしい値になる。生存圏データベースの値はすでに補正済みということ？
                    corrected_values = values

                    # Convert temperature to Celsius if the variable is 'temp'
                    if var_name == 'temp':
                        corrected_values -= 273.15  # Convert Kelvin to Celsius

                    data[var_name] = corrected_values
                except Exception as e:
                    print(f"警告: 変数 '{var_name}' の抽出中にエラーが発生しました: {e}")
                    data[var_name] = np.full(len(time_values), np.nan)  # 欠損値で埋める

            # Calculate wind direction and speed
            data['wind_direction'] = (270 - np.degrees(np.arctan2(data['v'], data['u']))) % 360
            data['wind_speed'] = np.sqrt(data['u']**2 + data['v']**2)
            
            # 地点ごとのディレクトリを作成
            target_dir = os.path.join(output_dir, target_name)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            
            # データをCSVに変換（年ディレクトリを作成）
            target_year_dir = os.path.join(target_dir, year_str)
            if not os.path.exists(target_year_dir):
                os.makedirs(target_year_dir)
            
            # 新しい命名規則：YYYYMMDDの形式で年を含む
            csv_file_path = os.path.join(target_year_dir, f"{full_date_str}.csv")
            
            # データフレームに変換してCSVとして保存
            df = pd.DataFrame(data)
            df.to_csv(csv_file_path, index=False)
            
            print(f"データを保存しました: {csv_file_path} (指定座標: {target_lat}, {target_lon}, 実際のグリッド: {actual_lat}, {actual_lon})")
            
        return True
    
    except Exception as e:
        print(f"エラー: ファイル {nc_file} の処理中に問題が発生しました: {e}")
        return False

def combine_csv_files(csv_base_dir, target_name, combine_start_date, combine_end_date, output_dir):
    """特定の地点の期間内のCSVファイルを結合し、統計データを作成する。
    各気象要素の特性に応じた適切な統計処理を行う。"""
    try:
        start_dt = datetime.strptime(combine_start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(combine_end_date, '%Y-%m-%d')
        
        target_dir = os.path.join(csv_base_dir, target_name)
        print(f"処理対象ディレクトリ: {target_dir}")
        
        if not os.path.exists(target_dir):
            print(f"エラー: 地点 '{target_name}' のデータディレクトリが見つかりません: {target_dir}")
            return False

        # 結合先ディレクトリの作成
        combined_dir = os.path.join(output_dir, 'combined')
        daily_dir = os.path.join(output_dir, 'daily')
        monthly_dir = os.path.join(output_dir, 'monthly')
        
        for directory in [combined_dir, daily_dir, monthly_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)

        # 期間内のファイルを検索
        all_files = []
        for year in range(start_dt.year, end_dt.year + 1):
            year_dir = os.path.join(target_dir, str(year))
            if os.path.exists(year_dir):
                for file in os.listdir(year_dir):
                    if file.endswith('.csv'):
                        try:
                            file_date = datetime.strptime(file.split('.')[0], '%Y%m%d')
                            if start_dt <= file_date <= end_dt:
                                all_files.append(os.path.join(year_dir, file))
                        except ValueError:
                            # 日付形式が異なる場合はスキップ
                            continue

        if not all_files:
            print(f"警告: 期間 {combine_start_date} から {combine_end_date} の地点 '{target_name}' のCSVファイルが見つかりません。")
            return False

        # ファイルを結合
        print(f"地点 '{target_name}' のデータを結合中... ({len(all_files)} ファイル)")
        
        # ファイルを日付順にソート
        all_files.sort()
        
        # 全データの結合
        all_data = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
        
        # 結合ファイルの保存
        start_str = combine_start_date.replace('-', '')
        end_str = combine_end_date.replace('-', '')
        combined_file = os.path.join(combined_dir, f"{target_name}_{start_str}-{end_str}.csv")
        all_data.to_csv(combined_file, index=False)
        print(f"結合データを保存しました: {combined_file}")
        
        # 日別・月別統計の計算
        all_data['datetime'] = pd.to_datetime(all_data['time'])
        all_data['date'] = all_data['datetime'].dt.date
        
        print("日別統計を計算中...")
        # 気象変数ごとに異なる統計処理を行う
        
        # 1. 日別統計の計算
        # 変数ごとの処理を定義
        daily_stats = {}
        
        # グループ化
        daily_groups = all_data.groupby('date')
        
        # 数値列の特定（風向を除く）
        numeric_columns = all_data.select_dtypes(include=[np.number]).columns.tolist()
        if 'wind_direction' in numeric_columns:
            numeric_columns.remove('wind_direction')
        
        # a. 平均値を計算する変数（気温、気圧、相対湿度など）
        mean_variables = ['temp', 'psea', 'sp', 'rh', 'dswrf', 'grid_latitude', 'grid_longitude']
        for var in mean_variables:
            if var in all_data.columns:
                daily_stats[f'{var}_mean'] = daily_groups[var].mean()
        
        # b. 積算値を計算する変数（降水量）
        sum_variables = ['r1h']
        for var in sum_variables:
            if var in all_data.columns:
                daily_stats[f'{var}_sum'] = daily_groups[var].sum()
        
        # c. 最大値・最小値を計算する変数（気温、風速など）
        max_min_variables = ['temp', 'wind_speed']
        for var in max_min_variables:
            if var in all_data.columns:
                daily_stats[f'{var}_max'] = daily_groups[var].max()
                daily_stats[f'{var}_min'] = daily_groups[var].min()
        
        # d. 風向の処理（最頻値と平均風向）
        if 'wind_direction' in all_data.columns and 'u' in all_data.columns and 'v' in all_data.columns:
            # 最頻値（10度ごとのビンに分類して計算）
            def get_most_frequent_direction(group):
                # 10度ごとのビンに分類
                bins = list(range(0, 361, 10))
                bin_labels = [f"{i}" for i in range(0, 360, 10)]
                binned = pd.cut(group, bins=bins, labels=bin_labels, include_lowest=True, right=False)
                # 最頻値を計算
                most_common = binned.value_counts().idxmax()
                return float(most_common) + 5  # ビンの中央値
            
            daily_stats['wind_direction_mode'] = daily_groups['wind_direction'].apply(get_most_frequent_direction)
            
            # ベクトル平均（U, V成分から計算）
            daily_u_mean = daily_groups['u'].mean()
            daily_v_mean = daily_groups['v'].mean()
            daily_stats['wind_direction_vector'] = (270 - np.degrees(np.arctan2(daily_v_mean, daily_u_mean))) % 360
        
        # e. その他のカスタム統計（晴れの時間、曇りの時間など）
        if 'ncld' in all_data.columns:
            # 晴れの時間（雲量3未満の時間数）
            daily_stats['clear_hours'] = daily_groups['ncld'].apply(lambda x: sum(x < 3))
            # 曇りの時間（雲量7以上の時間数）
            daily_stats['cloudy_hours'] = daily_groups['ncld'].apply(lambda x: sum(x >= 7))
        
        # f. 降水日の判定（日降水量1mm以上）
        if 'r1h' in all_data.columns:
            daily_stats['precipitation_day'] = daily_groups['r1h'].sum() >= 1.0
        
        # 日別統計をデータフレームに変換
        daily_df = pd.DataFrame(daily_stats)
        
        # 結果の保存
        daily_file = os.path.join(daily_dir, f"{target_name}_{start_str}-{end_str}_daily.csv")
        daily_df.to_csv(daily_file)
        print(f"日別統計データを保存しました: {daily_file}")
        
        # 2. 月別統計の計算
        print("月別統計を計算中...")
        all_data['month'] = all_data['datetime'].dt.to_period('M')
        monthly_groups = all_data.groupby('month')
        
        monthly_stats = {}
        
        # a. 平均値を計算する変数
        for var in mean_variables:
            if var in all_data.columns:
                monthly_stats[f'{var}_mean'] = monthly_groups[var].mean()
        
        # b. 積算値を計算する変数
        for var in sum_variables:
            if var in all_data.columns:
                monthly_stats[f'{var}_sum'] = monthly_groups[var].sum()
                
        # c. 最大値・最小値を計算する変数
        for var in max_min_variables:
            if var in all_data.columns:
                monthly_stats[f'{var}_max'] = monthly_groups[var].max()
                monthly_stats[f'{var}_min'] = monthly_groups[var].min()
        
        # d. 風向の処理
        if 'wind_direction' in all_data.columns and 'u' in all_data.columns and 'v' in all_data.columns:
            # ベクトル平均
            monthly_u_mean = monthly_groups['u'].mean()
            monthly_v_mean = monthly_groups['v'].mean()
            monthly_stats['wind_direction_vector'] = (270 - np.degrees(np.arctan2(monthly_v_mean, monthly_u_mean))) % 360
        
        # e. 月間降水日数
        # 日別統計から月ごとの降水日数を計算
        if 'precipitation_day' in daily_df.columns:
            # 日付から月を抽出
            daily_df['month'] = pd.to_datetime(daily_df.index).to_period('M')
            # 月ごとの降水日数をカウント
            precip_days = daily_df.groupby('month')['precipitation_day'].sum()
            monthly_stats['precipitation_days'] = precip_days
        
        # 月別統計をデータフレームに変換
        monthly_df = pd.DataFrame(monthly_stats)
        
        # 結果の保存
        monthly_file = os.path.join(monthly_dir, f"{target_name}_{start_str}-{end_str}_monthly.csv")
        monthly_df.to_csv(monthly_file)
        print(f"月別統計データを保存しました: {monthly_file}")
        
        return True
        
    except Exception as e:
        print(f"エラー: CSVファイルの結合中に問題が発生しました: {e}")
        import traceback
        traceback.print_exc()
        return False

def process_download_and_csv(config_file):
    """設定ファイルに基づいてデータのダウンロードとCSV変換を実行する"""
    with open(config_file, 'r', encoding='utf-8') as f:
        config = json.load(f)

    start_date = config['download_start_date']
    end_date = config['download_end_date']
    targets = config['targets']
    output_dir = config['output_directory']
    skip_existing = config.get('skip_existing_files', True)

    # ストレージ要件の計算
    calculate_storage_requirements(start_date, end_date, targets=targets)
    
    # ユーザー確認
    if 'auto_confirm' not in config or not config['auto_confirm']:
        confirm = input("\n続行しますか？ (y/n): ")
        if confirm.lower() != 'y':
            print("処理を中止しました。")
            return False

    # ディレクトリの準備
    save_dir = os.path.join(output_dir, 'netcdf')
    csv_dir = os.path.join(output_dir, 'csv')

    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
    if not os.path.exists(csv_dir):
        os.makedirs(csv_dir)

    # データをダウンロード
    download_msm_data(start_date, end_date, save_dir, skip_existing)

    # 各netCDFファイルから各地点のデータを抽出
    print("\n各地点のデータを抽出しています...")
    processed_count = 0
    skipped_count = 0
    failed_count = 0
    
    for year_dir in sorted(os.listdir(save_dir)):
        year_path = os.path.join(save_dir, year_dir)
        if os.path.isdir(year_path):
            for nc_file in sorted(os.listdir(year_path)):
                if nc_file.endswith('.nc'):
                    nc_file_path = os.path.join(year_path, nc_file)
                    
                    # 既存の抽出結果をチェック (全地点のファイルが存在するか)
                    all_extracted = True
                    for target_name in targets.keys():
                        target_year_dir = os.path.join(csv_dir, target_name, year_dir)
                        csv_file_path = os.path.join(target_year_dir, f"{os.path.splitext(nc_file)[0]}.csv")
                        if not os.path.exists(csv_file_path) or os.path.getsize(csv_file_path) == 0:
                            all_extracted = False
                            break
                    
                    if skip_existing and all_extracted:
                        print(f"既存の抽出結果をスキップ: {nc_file}")
                        skipped_count += 1
                    else:
                        print(f"処理中: {nc_file}")
                        if extract_msm_data_to_csv(nc_file_path, targets, csv_dir):
                            processed_count += 1
                        else:
                            failed_count += 1
    
    print(f"\nデータ抽出完了:")
    print(f"- 処理成功: {processed_count} ファイル")
    print(f"- スキップ: {skipped_count} ファイル")
    if failed_count > 0:
        print(f"- 処理失敗: {failed_count} ファイル")
    
    return True

def process_combine_csv(config_file):
    """設定ファイルに基づいてCSVファイルの結合処理を実行する"""
    with open(config_file, 'r', encoding='utf-8') as f:
        config = json.load(f)

    combine_start_date = config['combine_start_date']
    combine_end_date = config['combine_end_date']
    targets = config['targets']
    output_dir = config['output_directory']
    csv_dir = os.path.join(output_dir, 'csv')
    stats_dir = os.path.join(output_dir, 'statistics')
    
    if not os.path.exists(stats_dir):
        os.makedirs(stats_dir)

    success_count = 0
    failed_count = 0
    
    for target_name in targets.keys():
        print(f"\n地点 '{target_name}' のデータを結合中...")
        if combine_csv_files(csv_dir, target_name, combine_start_date, combine_end_date, stats_dir):
            success_count += 1
        else:
            failed_count += 1
    
    print(f"\nデータ結合完了:")
    print(f"- 成功: {success_count} 地点")
    if failed_count > 0:
        print(f"- 失敗: {failed_count} 地点")
    
    return success_count > 0

def main():
    parser = argparse.ArgumentParser(description="MSMデータ処理スクリプト")
    parser.add_argument("command", choices=["download", "combine"], help="操作を指定: 'download'でデータをダウンロードして処理、'combine'でCSVを結合")
    parser.add_argument("config", help="JSONの設定ファイルへのパス")
    
    args = parser.parse_args()

    print(f"MSMデータ処理ツール - コマンド: {args.command}")
    
    if args.command == "download":
        process_download_and_csv(args.config)
    elif args.command == "combine":
        process_combine_csv(args.config)

if __name__ == "__main__":
    main()
