# import netCDF4 as nc

# # NetCDFファイルを開く
# nc_file = nc.Dataset('output/netcdf/2025/0101.nc')

# # r1h変数のメタデータを確認
# if 'r1h' in nc_file.variables:
#     r1h_var = nc_file.variables['r1h']
#     print("r1h変数の属性:")
#     for attr_name in r1h_var.ncattrs():
#         print(f"  {attr_name}: {r1h_var.getncattr(attr_name)}")
    
#     # 特に以下の属性を確認
#     # - _FillValue: 欠測値を示す値
#     # - valid_range: 有効な値の範囲
#     # - scale_factor: スケール係数（生データから物理量への変換に使用）
#     # - add_offset: オフセット値





import netCDF4 as nc
import os
import numpy as np

def display_netcdf_metadata(nc_file_path):
    """
    NetCDFファイルの詳細なメタデータを表示する関数
    
    Parameters:
    -----------
    nc_file_path : str
        NetCDFファイルのパス
    """
    try:
        # NetCDFファイルを開く
        dataset = nc.Dataset(nc_file_path, 'r')
        
        # ファイル情報
        print("=" * 80)
        print(f"ファイル名: {os.path.basename(nc_file_path)}")
        print("=" * 80)
        
        # グローバル属性
        print("\n=== グローバル属性 ===")
        for attr_name in dataset.ncattrs():
            print(f"{attr_name}: {dataset.getncattr(attr_name)}")
        
        # 次元情報
        print("\n=== 次元情報 ===")
        for dim_name, dimension in dataset.dimensions.items():
            print(f"{dim_name}: サイズ={len(dimension)}, 無制限={dimension.isunlimited()}")
        
        # 変数情報
        print("\n=== 変数情報 ===")
        for var_name, variable in dataset.variables.items():
            print("\n" + "-" * 50)
            print(f"変数名: {var_name}")
            print(f"データ型: {variable.dtype}")
            print(f"次元: {variable.dimensions}")
            print(f"形状: {variable.shape}")
            
            # 変数の属性
            print("属性:")
            for attr_name in variable.ncattrs():
                attr_value = variable.getncattr(attr_name)
                
                # 配列の場合は短く表示
                if isinstance(attr_value, np.ndarray) and attr_value.size > 10:
                    print(f"  {attr_name}: 配列({attr_value.shape})")
                else:
                    print(f"  {attr_name}: {attr_value}")
            
            # 変数のデータの要約（大きなデータの場合は一部のみ）
            try:
                if len(variable.shape) == 0:
                    # スカラー値
                    print(f"値: {variable[...]}")
                elif variable.size <= 10:
                    # 小さなデータは全て表示
                    print(f"データ: {variable[...]}")
                else:
                    # 大きなデータは最初と最後の数値、最大値、最小値を表示
                    data = variable[...]
                    if hasattr(data, 'mask'):
                        # マスクされた配列の場合
                        valid_data = data[~data.mask]
                        if valid_data.size > 0:
                            print(f"データ要約（マスクされたデータ）:")
                            print(f"  最初のいくつか: {valid_data.flat[:5]}")
                            print(f"  最後のいくつか: {valid_data.flat[-5:]}")
                            print(f"  最小値: {np.nanmin(valid_data)}")
                            print(f"  最大値: {np.nanmax(valid_data)}")
                            print(f"  有効データ数: {valid_data.size}/{data.size}")
                        else:
                            print("すべてのデータがマスクされています")
                    else:
                        # 通常の配列
                        print(f"データ要約:")
                        print(f"  最初のいくつか: {data.flat[:5]}")
                        print(f"  最後のいくつか: {data.flat[-5:]}")
                        
                        # NaNを処理
                        if np.issubdtype(data.dtype, np.floating):
                            print(f"  最小値: {np.nanmin(data)}")
                            print(f"  最大値: {np.nanmax(data)}")
                            print(f"  NaN数: {np.isnan(data).sum()}/{data.size}")
                        else:
                            print(f"  最小値: {np.min(data)}")
                            print(f"  最大値: {np.max(data)}")
                        
                        # 特定の値の出現回数を確認（欠測値や特殊値の可能性があるもの）
                        unique_values, counts = np.unique(data, return_counts=True)
                        if unique_values.size <= 10:
                            for val, count in zip(unique_values, counts):
                                print(f"  値 {val} の出現回数: {count}")
                        else:
                            # 頻度の高い上位5つの値を表示
                            sorted_indices = np.argsort(counts)[::-1]
                            print("  最頻値（上位5つ）:")
                            for i in range(min(5, len(sorted_indices))):
                                idx = sorted_indices[i]
                                print(f"    値 {unique_values[idx]}: {counts[idx]}回")
                            
                            # 特殊値の可能性がある値を検索
                            special_values = [200, -999, -9999, 9999, 999]
                            for val in special_values:
                                if val in unique_values:
                                    idx = np.where(unique_values == val)[0][0]
                                    print(f"  特殊値の可能性がある {val} の出現回数: {counts[idx]}")
            except Exception as e:
                print(f"データの要約中にエラーが発生しました: {e}")
        
        # ファイルの構造情報を表示
        print("\n=== ファイル構造 ===")
        print(dataset)
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")
    
    finally:
        # ファイルを閉じる
        if 'dataset' in locals():
            dataset.close()
            print("\nファイルを閉じました")


def analyze_msm_precipitation(nc_file_path):
    """
    MSMの降水量データを特に詳しく分析する関数
    
    Parameters:
    -----------
    nc_file_path : str
        NetCDFファイルのパス
    """
    try:
        # NetCDFファイルを開く
        dataset = nc.Dataset(nc_file_path, 'r')
        
        # 降水量変数を探す（一般的な名前）
        precip_vars = [var for var in dataset.variables 
                      if var.lower() in ['r1h', 'rain', 'precipitation', 'precip', 'tp']]
        
        if not precip_vars:
            print("降水量変数が見つかりませんでした")
            return
        
        print("\n" + "=" * 80)
        print("降水量データの詳細分析")
        print("=" * 80)
        
        for var_name in precip_vars:
            variable = dataset.variables[var_name]
            print(f"\n変数名: {var_name}")
            
            # データ取得
            data = variable[...]
            
            # 基本統計
            print("\n基本統計:")
            if hasattr(data, 'mask'):
                # マスクされたデータの場合
                valid_data = data[~data.mask]
                print(f"有効データ数: {valid_data.size}/{data.size}")
                if valid_data.size > 0:
                    print(f"最小値: {np.min(valid_data)}")
                    print(f"最大値: {np.max(valid_data)}")
                    print(f"平均値: {np.mean(valid_data)}")
                    print(f"中央値: {np.median(valid_data)}")
            else:
                print(f"最小値: {np.min(data)}")
                print(f"最大値: {np.max(data)}")
                print(f"平均値: {np.mean(data)}")
                print(f"中央値: {np.median(data)}")
            
            # 値の分布
            unique_values, counts = np.unique(data, return_counts=True)
            print(f"\n一意な値の数: {len(unique_values)}")
            
            # 最頻値
            idx_max = np.argmax(counts)
            print(f"最頻値: {unique_values[idx_max]} (出現回数: {counts[idx_max]})")
            
            # 特定の値の検索と分析
            special_values = [200, -999, -9999, 9999, 999]
            for val in special_values:
                if val in unique_values:
                    idx = np.where(unique_values == val)[0][0]
                    count = counts[idx]
                    percent = (count / data.size) * 100
                    print(f"特殊値の可能性がある {val} の出現: {count}回 ({percent:.2f}%)")
                    
                    # 特殊値の位置パターンを分析（時間軸に沿った分布）
                    if 'time' in variable.dimensions:
                        time_idx = variable.dimensions.index('time')
                        if time_idx >= 0 and len(data.shape) > time_idx:
                            # 時間ごとの特殊値の出現回数
                            if len(data.shape) == 1:  # 1次元配列の場合
                                time_pattern = [1 if val == x else 0 for x in data]
                            else:  # 多次元配列の場合
                                # 各時間ステップで値が出現するかどうかをチェック
                                time_slices = []
                                for t in range(data.shape[time_idx]):
                                    # 各次元に対応するスライスを作成
                                    idx = [slice(None)] * len(data.shape)
                                    idx[time_idx] = t
                                    time_slice = data[tuple(idx)]
                                    has_val = np.any(time_slice == val)
                                    time_slices.append(1 if has_val else 0)
                                time_pattern = time_slices
                            
                            print(f"時間軸に沿った {val} の出現パターン: {time_pattern}")
                            
                            # パターンの周期性を分析
                            if sum(time_pattern) > 1:
                                indices = [i for i, x in enumerate(time_pattern) if x == 1]
                                diffs = [indices[i+1] - indices[i] for i in range(len(indices)-1)]
                                if diffs:
                                    print(f"隣接する出現間隔: {diffs}")
                                    if len(set(diffs)) == 1:
                                        print(f"周期的なパターンを検出: {diffs[0]} 単位間隔で出現")
            
            # 属性から特殊値や欠測値に関する情報を探す
            for attr_name in variable.ncattrs():
                attr_value = variable.getncattr(attr_name)
                attr_lower = attr_name.lower()
                if any(x in attr_lower for x in ['fill', 'missing', 'invalid', 'special']):
                    print(f"\n特殊値に関する属性: {attr_name} = {attr_value}")
                    
                    # この属性で指定された値の出現回数を確認
                    if np.isscalar(attr_value):
                        if attr_value in unique_values:
                            idx = np.where(unique_values == attr_value)[0][0]
                            print(f"この値の出現回数: {counts[idx]}")
            
    except Exception as e:
        print(f"降水量データ分析中にエラーが発生しました: {e}")
    
    finally:
        # ファイルを閉じる
        if 'dataset' in locals():
            dataset.close()


# 使用例
if __name__ == "__main__":
    # NetCDFファイルのパスを指定
    nc_file_path = "output/netcdf/2025/0101.nc"
    
    # 引数としてファイルパスを受け取る場合
    import sys
    if len(sys.argv) > 1:
        nc_file_path = sys.argv[1]
    
    print(f"ファイル {nc_file_path} のメタデータを表示します...\n")
    
    # メタデータを表示
    display_netcdf_metadata(nc_file_path)
    
    # 降水量データの詳細分析
    analyze_msm_precipitation(nc_file_path)
