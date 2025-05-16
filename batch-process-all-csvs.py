#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MSMデータの全CSVファイルに対して特殊値処理を一括実行するスクリプト

使用方法:
    python batch_process_all_csvs.py --input-dir ./output/csv --output-dir ./output/csv_fixed

このスクリプトは以下の処理を行います:
1. 指定されたディレクトリ内のすべての地点・年のフォルダを探索
2. 各フォルダ内のCSVファイルに対して特殊値処理を実行
3. 処理結果を指定された出力ディレクトリに同じ構造で保存
"""

import os
import sys
import time
import argparse
import concurrent.futures
import pandas as pd
import numpy as np
from datetime import datetime

# fix_r1h_csv.pyからの関数のインポート
# もし別ファイルの場合は、このスクリプトと同じディレクトリにfix_r1h_csv.pyを置く
try:
    from fix_r1h_csv import process_r1h_timeseries
except ImportError:
    # 必要な関数を直接定義
    def process_r1h_timeseries(input_file, output_file, method='nan', r1h_column='r1h', time_column='time', verbose=True):
        """
        時系列として整理されたCSVファイルを効率的に処理する関数
        特に時系列に沿った補間に最適化されています
        
        Parameters:
        -----------
        input_file : str
            入力CSVファイルのパス
        output_file : str
            出力CSVファイルのパス
        method : str, default='nan'
            特殊値の処理方法 ('nan', 'zero', 'interp')
        r1h_column : str, default='r1h'
            降水量データの列名
        time_column : str, default='time'
            時間データの列名
        verbose : bool, default=True
            詳細出力を表示するかどうか
        
        Returns:
        --------
        bool
            処理が成功したかどうか
        """
        try:
            # CSVファイルを読み込む
            if verbose:
                print(f"処理中: {input_file}")
            
            df = pd.read_csv(input_file)
            
            if r1h_column not in df.columns:
                if verbose:
                    print(f"警告: '{r1h_column}'列が存在しません")
                return False
            
            # 時間列がある場合は日時型に変換
            if time_column in df.columns:
                try:
                    df[time_column] = pd.to_datetime(df[time_column])
                    # 時間でソート
                    df = df.sort_values(time_column)
                except:
                    if verbose:
                        print(f"警告: 時間列の変換でエラーが発生しました")
            
            # 特殊値の処理
            special_mask = np.isclose(df[r1h_column], 200, rtol=1e-10, atol=1e-10)
            special_count = special_mask.sum()
            
            if special_count > 0:
                if method == 'nan':
                    # NaNに置換
                    df.loc[special_mask, r1h_column] = np.nan
                
                elif method == 'zero':
                    # 0に置換
                    df.loc[special_mask, r1h_column] = 0.0
                
                elif method == 'interp':
                    # NaNに置換して補間
                    df.loc[special_mask, r1h_column] = np.nan
                    df[r1h_column] = df[r1h_column].interpolate(method='linear', limit_direction='both')
                    df[r1h_column] = df[r1h_column].fillna(0)
                
                # 極小な負値を0に設定（オプション）
                small_negative_mask = df[r1h_column] < 0.0001
                if small_negative_mask.sum() > 0:
                    df.loc[small_negative_mask, r1h_column] = 0.0
            
            # 処理済みデータを保存
            output_dir = os.path.dirname(output_file)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
                
            df.to_csv(output_file, index=False)
            
            return True
            
        except Exception as e:
            if verbose:
                print(f"エラー: ファイル {input_file} の処理中に問題が発生しました: {e}")
            return False


def find_csv_files(base_dir):
    """
    指定されたディレクトリ内のすべてのCSVファイルを再帰的に検索
    
    Parameters:
    -----------
    base_dir : str
        検索を開始するベースディレクトリ
    
    Returns:
    --------
    list
        発見されたCSVファイルのパスのリスト
    """
    csv_files = []
    
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    
    return csv_files


def process_csv_batch(input_files, output_dir, method='nan', r1h_column='r1h', time_column='time',
                     parallel=True, max_workers=None, verbose=True):
    """
    複数のCSVファイルをバッチ処理する関数
    
    Parameters:
    -----------
    input_files : list
        処理するCSVファイルのパスのリスト
    output_dir : str
        出力ディレクトリのパス
    method : str, default='nan'
        特殊値の処理方法 ('nan', 'zero', 'interp')
    r1h_column : str, default='r1h'
        降水量データの列名
    time_column : str, default='time'
        時間データの列名
    parallel : bool, default=True
        並列処理を行うかどうか
    max_workers : int, default=None
        並列処理の最大ワーカー数（Noneの場合はCPUコア数×5）
    verbose : bool, default=True
        詳細出力を表示するかどうか
    
    Returns:
    --------
    tuple
        (成功数, 失敗数, 処理時間)
    """
    start_time = time.time()
    
    # 入力ディレクトリと出力ディレクトリのマッピングを作成
    output_files = []
    input_dir = os.path.commonpath([os.path.dirname(f) for f in input_files])
    
    for input_file in input_files:
        rel_path = os.path.relpath(input_file, input_dir)
        output_file = os.path.join(output_dir, rel_path)
        output_files.append(output_file)
    
    success_count = 0
    failure_count = 0
    
    if parallel:
        # 並列処理
        if max_workers is None:
            # デフォルトはCPUコア数の5倍（I/O待ちが多いため）
            import multiprocessing
            max_workers = multiprocessing.cpu_count() * 5
        
        if verbose:
            print(f"並列処理を開始: 最大ワーカー数 = {max_workers}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # ファイルごとに処理を並列実行
            future_to_file = {
                executor.submit(
                    process_r1h_timeseries, 
                    input_file, 
                    output_file, 
                    method, 
                    r1h_column, 
                    time_column, 
                    False  # 並列処理時は詳細出力を抑制
                ): input_file 
                for input_file, output_file in zip(input_files, output_files)
            }
            
            # タスク完了を待機し、進捗状況を表示
            total_files = len(input_files)
            completed = 0
            
            for future in concurrent.futures.as_completed(future_to_file):
                input_file = future_to_file[future]
                try:
                    success = future.result()
                    if success:
                        success_count += 1
                    else:
                        failure_count += 1
                except Exception as e:
                    if verbose:
                        print(f"エラー: ファイル {input_file} の処理中に例外が発生しました: {e}")
                    failure_count += 1
                
                completed += 1
                if verbose and completed % 100 == 0:
                    progress = (completed / total_files) * 100
                    elapsed = time.time() - start_time
                    remaining = (elapsed / completed) * (total_files - completed) if completed > 0 else 0
                    print(f"進捗: {completed}/{total_files} ファイル ({progress:.1f}%) - "
                          f"経過時間: {elapsed:.1f}秒, 残り時間: {remaining:.1f}秒")
    else:
        # 逐次処理
        for i, (input_file, output_file) in enumerate(zip(input_files, output_files)):
            if verbose and (i+1) % 100 == 0:
                progress = ((i+1) / len(input_files)) * 100
                print(f"進捗: {i+1}/{len(input_files)} ファイル ({progress:.1f}%)")
            
            try:
                success = process_r1h_timeseries(
                    input_file, 
                    output_file, 
                    method, 
                    r1h_column, 
                    time_column, 
                    False  # 詳細出力を抑制
                )
                
                if success:
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as e:
                if verbose:
                    print(f"エラー: ファイル {input_file} の処理中に例外が発生しました: {e}")
                failure_count += 1
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    return success_count, failure_count, processing_time


def main():
    parser = argparse.ArgumentParser(description='MSMデータの全CSVファイルに対して特殊値処理を一括実行するスクリプト')
    parser.add_argument('--input-dir', default='./output/csv', help='入力CSVファイルがあるベースディレクトリ')
    parser.add_argument('--output-dir', default='./output/csv_fixed', help='処理済みCSVファイルを保存するディレクトリ')
    parser.add_argument('--method', choices=['nan', 'zero', 'interp'], default='nan',
                        help='特殊値の処理方法 (nan=NaNに置換, zero=0に置換, interp=線形補間で置換)')
    parser.add_argument('--r1h-column', default='r1h', help='降水量データの列名')
    parser.add_argument('--time-column', default='time', help='時間データの列名')
    parser.add_argument('--sequential', action='store_true', help='並列処理を無効にして逐次処理を行う')
    parser.add_argument('--max-workers', type=int, default=None, help='並列処理の最大ワーカー数')
    parser.add_argument('--pattern', default='*.csv', help='処理対象とするファイルのパターン (デフォルト: *.csv)')
    parser.add_argument('--station', help='特定の観測地点のみを処理する場合、その地点名')
    parser.add_argument('--year', help='特定の年のみを処理する場合、その年')
    parser.add_argument('--quiet', action='store_true', help='詳細出力を表示しない')
    
    args = parser.parse_args()
    
    # 入力ディレクトリの存在確認
    if not os.path.exists(args.input_dir):
        print(f"エラー: 入力ディレクトリ '{args.input_dir}' が見つかりません")
        return 1
    
    verbose = not args.quiet
    
    # 処理対象のCSVファイルを検索
    if args.station and args.year:
        # 特定の観測地点と年のCSVファイルを対象
        search_dir = os.path.join(args.input_dir, args.station, args.year)
        if not os.path.exists(search_dir):
            print(f"エラー: 指定されたディレクトリ '{search_dir}' が見つかりません")
            return 1
    elif args.station:
        # 特定の観測地点のCSVファイルを対象
        search_dir = os.path.join(args.input_dir, args.station)
        if not os.path.exists(search_dir):
            print(f"エラー: 指定された観測地点のディレクトリ '{search_dir}' が見つかりません")
            return 1
    elif args.year:
        # 特定の年のCSVファイルを対象（全観測地点）
        # 年ディレクトリは観測地点ディレクトリの下にあるため、複数のパスを検索
        station_dirs = [os.path.join(args.input_dir, d) for d in os.listdir(args.input_dir) 
                        if os.path.isdir(os.path.join(args.input_dir, d))]
        
        search_dirs = []
        for station_dir in station_dirs:
            year_dir = os.path.join(station_dir, args.year)
            if os.path.exists(year_dir):
                search_dirs.append(year_dir)
        
        if not search_dirs:
            print(f"エラー: 指定された年 '{args.year}' のディレクトリが見つかりません")
            return 1
        
        # 複数のディレクトリから検索結果をマージ
        all_csv_files = []
        for search_dir in search_dirs:
            csv_files = find_csv_files(search_dir)
            all_csv_files.extend(csv_files)
        
        csv_files = all_csv_files
    else:
        # すべてのCSVファイルを対象
        search_dir = args.input_dir
        csv_files = find_csv_files(search_dir)
    
    # yearとstation両方指定されていない場合の検索（通常ケース）
    if not hasattr(locals(), 'csv_files'):
        csv_files = find_csv_files(search_dir)
    
    # 処理対象のCSVファイル数を確認
    if not csv_files:
        print(f"エラー: 処理対象のCSVファイルが見つかりません")
        return 1
    
    if verbose:
        print(f"MSMデータ一括処理ツール")
        print(f"処理対象: {len(csv_files)} ファイル")
        print(f"処理方法: {args.method}")
        
        # 処理対象の内訳を表示
        stations = set()
        years = set()
        
        for csv_file in csv_files:
            # パスからステーション名と年を抽出
            parts = csv_file.split(os.sep)
            for i, part in enumerate(parts):
                if i > 0 and parts[i-1] == 'csv':
                    stations.add(part)
                if part.isdigit() and len(part) == 4:
                    years.add(part)
        
        print(f"対象観測地点: {', '.join(sorted(stations))}")
        print(f"対象年: {', '.join(sorted(years))}")
    
    # 出力ディレクトリの作成
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    # 処理の実行
    success_count, failure_count, processing_time = process_csv_batch(
        csv_files,
        args.output_dir,
        method=args.method,
        r1h_column=args.r1h_column,
        time_column=args.time_column,
        parallel=not args.sequential,
        max_workers=args.max_workers,
        verbose=verbose
    )
    
    # 処理結果の表示
    if verbose:
        print(f"\n処理完了:")
        print(f"- 成功: {success_count} ファイル")
        print(f"- 失敗: {failure_count} ファイル")
        print(f"- 合計処理時間: {processing_time:.2f}秒")
        
        # 処理速度の計算
        if processing_time > 0:
            files_per_second = (success_count + failure_count) / processing_time
            print(f"- 処理速度: {files_per_second:.2f}ファイル/秒")
    
    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
