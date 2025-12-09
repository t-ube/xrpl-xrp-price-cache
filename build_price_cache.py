import requests
import json
import os
import time
from datetime import datetime, timedelta, timezone

BINANCE_BASE_URL = "https://api.binance.com"
SYMBOL = "XRPUSDT"
INTERVAL = "1d"
LIMIT = 1000

def dt_to_millis(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def fetch_klines(symbol: str, interval: str, start: datetime, end: datetime):
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
            "limit": LIMIT,
        }
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        klines = r.json()

        if not klines:
            break

        all_klines.extend(klines)

        last_close_time = klines[-1][6]  # close time (ms)
        next_start = last_close_time + 1
        if next_start >= end_ms:
            break
        start_ms = next_start

        time.sleep(0.2)

        if len(klines) < LIMIT:
            break

    return all_klines


def build_price_cache(dt_start: datetime, dt_end: datetime):
    """
    最小構成の価格キャッシュ:
    {
      "2022-10-01": 0.4980,
      "2022-10-02": 0.5075
    }
    """
    # end は +1 日して取得範囲に余裕を作る
    fetch_start = datetime(dt_start.year, dt_start.month, dt_start.day, tzinfo=timezone.utc)
    fetch_end   = datetime(dt_end.year, dt_end.month, dt_end.day, tzinfo=timezone.utc) + timedelta(days=1)

    print(f"💰 Binance日足から {dt_start.date()} ～ {dt_end.date()} を取得します…")

    klines = fetch_klines(SYMBOL, INTERVAL, fetch_start, fetch_end)

    cache = {}

    for k in klines:
        open_time_ms = k[0]
        close_price  = float(k[4])

        open_dt = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
        date_key = open_dt.strftime("%Y-%m-%d")

        # 範囲外はスキップ
        if not (dt_start.date() <= open_dt.date() <= dt_end.date()):
            continue

        cache[date_key] = close_price

    os.makedirs("cache", exist_ok=True)
    path = os.path.join(
        "cache",
        f"xrp_price_close_{SYMBOL}_{dt_start.date()}_{dt_end.date()}.json"
    )

    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=None, separators=(",", ":"))

    print(f"✅ 完了: {path} に保存しました ({len(cache)} days)")
    return cache


if __name__ == "__main__":
    dt_start = datetime(2022, 10, 1, tzinfo=timezone.utc)
    dt_end   = datetime(2025, 11, 30, tzinfo=timezone.utc)

    build_price_cache(dt_start, dt_end)
