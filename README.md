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




