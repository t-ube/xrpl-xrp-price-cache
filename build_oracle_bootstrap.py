"""
XRP オラクル用のブートストラップスクリプト

- Binance から XRPUSDT の日足（1d）を取得
- Frankfurter から USD/JPY の日次レートを取得
- それらをマージして R2 で使う JSON 形式に変換して保存

出力形式:

{
  "meta": {
    "version": 1,
    "last_date": "2025-12-01"
  },
  "daily": {
    "2025-10-01": { "USD": 0.51, "JPY": 76.23 },
    "2025-10-02": { "USD": 0.52, "JPY": 77.01 },
    ...
  }
}

"""

import json
import os
import time
from datetime import datetime, timedelta, timezone

import requests

# ===== 設定値 =====

BINANCE_BASE_URL = "https://api.binance.com"
BINANCE_SYMBOL = "XRPUSDT"
BINANCE_INTERVAL = "1d"
BINANCE_LIMIT = 1000

FRANKFURTER_BASE_URL = "https://api.frankfurter.app"

# 出力先ディレクトリ
OUTPUT_DIR = "./cache"

# ===== 共通ヘルパ =====


def dt_to_millis(dt: datetime) -> int:
    """datetime -> UNIX ミリ秒"""
    return int(dt.timestamp() * 1000)


def date_range(start: datetime, end: datetime):
    """日付のイテレータ（両端含む）"""
    cur = start
    while cur.date() <= end.date():
        yield cur
        cur += timedelta(days=1)


# ===== Binance から XRPUSDT 日足を取得 =====


def fetch_klines(
    symbol: str,
    interval: str,
    start: datetime,
    end: datetime,
    limit: int = BINANCE_LIMIT,
):
    """
    Binance の klines (XRPUSDT 1d) を start〜end でまとめて取得
    必要に応じて複数回リクエストして全件を返す
    """
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    start_ms = dt_to_millis(start)
    end_ms = dt_to_millis(end)

    all_klines = []

    while True:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        }
        print(f"[BINANCE] GET {url} {params}")
        res = requests.get(url, params=params, timeout=10)
        res.raise_for_status()
        klines = res.json()

        if not isinstance(klines, list) or len(klines) == 0:
            print("[BINANCE] 取得完了 (no more data)")
            break

        all_klines.extend(klines)

        # まだデータが続きそうなら、最後の open_time の次 ms から再取得
        last_open_time = klines[-1][0]
        if last_open_time >= end_ms or len(klines) < limit:
            break

        start_ms = last_open_time + 1
        time.sleep(0.2)  # レート制限緩和

    print(f"[BINANCE] 取得件数: {len(all_klines)}")
    return all_klines


def build_xrp_usd_daily(
    dt_start: datetime, dt_end: datetime
) -> dict[str, float]:
    """
    Binance の klines から、
    { "YYYY-MM-DD": usd_close } の dict を作る
    """
    klines = fetch_klines(BINANCE_SYMBOL, BINANCE_INTERVAL, dt_start, dt_end)
    daily_usd: dict[str, float] = {}

    for k in klines:
        # kline のフォーマットは https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
        open_time_ms = k[0]
        close_price = float(k[4])  # index 4 が close

        open_dt = datetime.fromtimestamp(
            open_time_ms / 1000, tz=timezone.utc
        )
        date_key = open_dt.strftime("%Y-%m-%d")

        if not (dt_start.date() <= open_dt.date() <= dt_end.date()):
            continue

        daily_usd[date_key] = close_price

    print(f"[XRP/USD] 日足件数: {len(daily_usd)}")
    return daily_usd


# ===== Frankfurter から USD/JPY 日次レートを取得 =====


def fetch_usd_jpy_timeseries(
    dt_start: datetime, dt_end: datetime
) -> dict[str, float]:
    """
    Frankfurter から USD→JPY の日次レートをまとめて取得

    レスポンス例:
    {
      "rates": {
        "2025-12-01": {"JPY": 146.23},
        "2025-12-02": {"JPY": 147.01},
        ...
      },
      ...
    }
    """
    start_str = dt_start.strftime("%Y-%m-%d")
    end_str = dt_end.strftime("%Y-%m-%d")
    url = f"{FRANKFURTER_BASE_URL}/{start_str}..{end_str}"
    params = {"from": "USD", "to": "JPY"}

    print(f"[FX] GET {url} {params}")
    res = requests.get(url, params=params, timeout=10)
    res.raise_for_status()
    data = res.json()

    rates = data.get("rates", {})
    out: dict[str, float] = {}
    for date_key, row in rates.items():
        JPY = row.get("JPY")
        if JPY is not None:
            out[date_key] = float(JPY)

    print(f"[FX] USD/JPY 日次件数: {len(out)}")
    return out


# ===== ブートストラップ本体 =====


def build_oracle_bootstrap(
    dt_start: datetime,
    dt_end: datetime,
    output_dir: str = OUTPUT_DIR,
) -> dict:
    """
    XRP オラクル用の daily JSON を構築してファイル保存する
    """
    os.makedirs(output_dir, exist_ok=True)

    print(f"[INFO] 期間: {dt_start.date()} 〜 {dt_end.date()}")

    # XRPUSDT 日足
    daily_usd = build_xrp_usd_daily(dt_start, dt_end)

    # USD/JPY 日次レート
    fx = fetch_usd_jpy_timeseries(dt_start, dt_end)

    daily: dict[str, dict] = {}
    missing_fx = 0
    last_rate = None  # 直近のレートを保持

    for d in sorted(daily_usd.keys()):
        usd_close = daily_usd[d]

        # その日にFXデータがあれば更新する
        if d in fx:
            last_rate = fx[d]
        else:
            # FXがない日（＝土日・祝日など）
            if last_rate is None:
                # まだ一度もレートが決まっていない初期部分だけスキップ
                print(f"[WARN] {d} の USD/JPY レートが無く、過去レートも無いのでスキップします。")
                missing_fx += 1
                continue
            # ここで何もせず last_rate をそのまま使う（＝前営業日のレートを引き継ぐ）

        jpy_close = usd_close * last_rate
        daily[d] = {
            "USD": usd_close,
            "JPY": jpy_close,
        }

    # メタ情報
    last_date = max(daily.keys()) if daily else None
    result = {
        "meta": {
            "version": 1,
            "last_date": last_date,
        },
        "daily": daily,
    }

    # ファイル名: xrp_oracle_daily_YYYY-MM-DD_YYYY-MM-DD.json
    filename = f"xrp_oracle_daily_{dt_start.date()}_{dt_end.date()}.json"
    path = os.path.join(output_dir, filename)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, separators=(",", ":"))

    print(
        f"\n[OK] {path} に保存しました "
        f"({len(daily)} 日分, FX欠損 {missing_fx} 日)"
    )
    return result


if __name__ == "__main__":
    # ★ ここを必要な期間に合わせて調整してください
    dt_start = datetime(2010, 1, 1, tzinfo=timezone.utc)
    dt_end = datetime(2025, 12, 1, tzinfo=timezone.utc)

    build_oracle_bootstrap(dt_start, dt_end)
