# MSM気象データ処理ツール

このツールは気象庁メソスケールモデル（MSM）のデータをダウンロードし，特定地点のデータを抽出して解析するためのPythonスクリプトです．京都大学生存圏データベースからMSMデータを取得し，指定した地点の気象情報をCSV形式で出力します．

## 機能

- MSMのnetCDFファイルのダウンロード
- 複数地点の気象データの抽出
- 日別・月別統計データの作成
- 既存ファイルの確認によるダウンロードの最適化
- 必要ストレージ容量の事前計算

## 必要条件

- Python 3.6以上
- 以下のPythonパッケージ:
  - netCDF4
  - pandas
  - numpy
  - argparse
  - shutil

```bash
pip install netCDF4 pandas numpy
```

## 設定ファイル

`config.json`ファイルで設定を行います．以下は設定例です：

```json
{
    "download_start_date": "2020-05-01",
    "download_end_date": "2020-05-02",
    "combine_start_date": "2020-05-01",
    "combine_end_date": "2020-05-02",
    "targets": {
        "yukikabe": {
            "latitude": 43.5789,
            "longitude": 144.5288,
            "description": "雪壁観測地点"
        },
        "kitami_city": {
            "latitude": 43.8041,
            "longitude": 143.8908,
            "description": "北見市中心部"
        }
    },
    "output_directory": "./output",
    "skip_existing_files": true,
    "auto_confirm": false
}
```

### 設定パラメータ

- `download_start_date`，`download_end_date`：ダウンロードする期間（YYYY-MM-DD形式）
- `combine_start_date`，`combine_end_date`：結合処理を行う期間（YYYY-MM-DD形式）
- `targets`：処理対象の地点情報
  - 各地点は名前をキーとし，緯度・経度・説明を含む
- `output_directory`：出力ディレクトリのパス
- `skip_existing_files`：既存ファイルをスキップするかどうか
- `auto_confirm`：ユーザー確認をスキップするかどうか

## 使用方法

### データのダウンロードと抽出

```bash
python makedata.py download config.json
```

このコマンドは以下の処理を行います：
1. 必要なストレージ容量を計算
2. MSMデータをダウンロード
3. 指定した地点のデータをCSVファイルに抽出

### データの結合と統計処理

```bash
python makedata.py combine config.json
```

このコマンドは以下の処理を行います：
1. 各地点の指定期間のCSVファイルを結合
2. 日別平均と月別平均を計算
3. 結果をCSVファイルに保存

### 既存CSVファイルの移行

古い形式のCSVファイル（MMDD.csv）を新しい形式（YYYYMMDD.csv）に移行するには：

```bash
python migrate_csv.py --dir ./output
```

## 出力ディレクトリ構造

```
output/
├── netcdf/                 # ダウンロードしたnetCDFファイル
│   └── YYYY/
│       └── MMDD.nc
├── csv/                    # 抽出したCSVファイル
│   └── [地点名]/
│       └── YYYY/
│           └── YYYYMMDD.csv
└── statistics/             # 統計データ
    ├── combined/           # 結合データ
    │   └── [地点名]_YYYYMMDD-YYYYMMDD.csv
    ├── daily/              # 日別統計
    │   └── [地点名]_YYYYMMDD-YYYYMMDD_daily.csv
    └── monthly/            # 月別統計
        └── [地点名]_YYYYMMDD-YYYYMMDD_monthly.csv
```

## 抽出されるデータ

- `time`：時間
- `grid_latitude`，`grid_longitude`：実際のグリッド座標
- `psea`：海面気圧
- `sp`：地上気圧
- `u`，`v`：風の東西・南北成分
- `temp`：気温（摂氏）
- `rh`：相対湿度
- `r1h`：1時間降水量
- `ncld`：雲量
- `dswrf`：下向き短波放射フラックス
- `wind_direction`：風向（度）
- `wind_speed`：風速（m/s）

## 注意点

- MSMデータのグリッド解像度は約5kmです．指定座標に最も近いグリッドポイントのデータが抽出されます．
- netCDFファイルのサイズは1日あたり約140MBです．長期間のダウンロードを行う場合は，十分なディスク容量を確保してください．
- ダウンロードは京都大学生存圏データベースのサーバーに負荷をかけるため，必要最小限に留めてください．
- 生成されるCSVファイルの命名規則は「YYYYMMDD.csv」形式を使用しています．

## ライセンス

このスクリプトは自由に使用・改変・再配布できますが，生存圏データベースのデータ利用に関しては京都大学の規約に従ってください．

## 謝辞

このツールは京都大学生存圏データベースが提供するMSMデータを利用しています．データの提供に感謝いたします．





## MSM

Hourlyデータのダウンロード
```Shell
python3 makedata.py combine ./config.json
```

データの結合, Dayly, Monthlyデータの作成. 
```Shell
python3 makedata.py combine ./config.json
```

## メモ
UV成分とは、U＝東西風・V＝南北風を表し、気象庁MSMなど数値予報GPVの風データは通常UV成分になっています。一般には東西風なら西風が正の値、南北風なら南風が正の値になっています。
https://qiita.com/Yoshiki443/items/6a4682bebdf87bd82cff

## ダウンロードのコマンド
curl -O http://database.rish.kyoto-u.ac.jp/arch/jmadata/data/gpv/netcdf/MSM-S/2022/0106.nc
curl -O http://database.rish.kyoto-u.ac.jp/arch/jmadata/data/gpv/latest/MSM-S.nc




