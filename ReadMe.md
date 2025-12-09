# Read Me

## はじめに
このプロジェクトはXRP価格をオラクルとしてキャッシュします。

## 保存先
キャッシュはCloudflare R2にストアします。

## ブートストラップを生成する
python build_oracle_bootstrap.py

## 差分埋めを行う
python fill_oracle_daily_diff.py

## R2自動処理
python fill_oracle_daily_r2.py

## 価格オラクルの仕組み
- Binance XRP/USDT 日足取得を行う。
- Frankfurter API（EUR基準）から EUR→JPY / EUR→USD を取り、USDJPY = (EURJPY / EURUSD) で算出する。
- データは["XRP/USDT","XRP/JPY"]の順序で保存される。
{"2022-10-01":[0.4754,68.77],"2022-10-02":[0.4485,64.88],"2022-10-03":[0.4621,66.96]}

1. 既存キャッシュを取得
2. 価格データ生成を実行
3. 既存キャッシュに追記
4. キャッシュをストア
5. この処理は一時間ごとに実行されます
