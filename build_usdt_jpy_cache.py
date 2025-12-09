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
        try:
            print(f"[DEBUG] Binance リクエスト: startTime={start_ms}, endTime={end_ms}")
            r = requests.get(url, params=params, timeout=10)
            r.raise_for_status()
        except Exception as e:
            print(f"[ERROR] Binance 取得失敗: {e}")
            return all_klines

        klines = r.json()
        if not klines:
            break

        all_klines.extend(klines)
        print(f"[INFO] Binance: {len(klines)} klines 取得（累計: {len(all_klines)}）")

        last_close_time = klines[-1][6]
        next_start = last_close_time + 1
        if next_start >= end_ms:
            break
        start_ms = next_start

        time.sleep(0.5)  # レート制限対策
        if len(klines) < LIMIT:
            break

    return all_klines


def fetch_usd_jpy_daily(dt_start: datetime, dt_end: datetime) -> dict:
    """
    Frankfurter API（EUR基準）から EUR→JPY / EUR→USD を取り、
    USDJPY = (EURJPY / EURUSD) で算出する。
    戻り値: {"YYYY-MM-DD": usd_jpy_rate}
    """
    url = "https://api.frankfurter.app/"
    rates = {}

    cur = dt_start
    while cur <= dt_end:
        date_str = cur.strftime("%Y-%m-%d")
        try:
            r = requests.get(f"{url}{date_str}?from=EUR&to=JPY,USD", timeout=5)
            r.raise_for_status()
            data = r.json()

            eur_jpy = data["rates"].get("JPY")
            eur_usd = data["rates"].get("USD")

            if eur_jpy and eur_usd:
                usd_jpy = eur_jpy / eur_usd
                rates[date_str] = float(usd_jpy)
                print(f"[FX] {date_str}: USDJPY={usd_jpy:.4f}")
            else:
                print(f"[WARN] {date_str}: レート不足 {data}")

        except Exception as e:
            print(f"[WARN] {date_str} 取得失敗: {e}")

        cur += timedelta(days=1)
        time.sleep(0.1)

    return rates

def build_price_cache(dt_start: datetime, dt_end: datetime):
    fetch_start = datetime(dt_start.year, dt_start.month, dt_start.day, tzinfo=timezone.utc)
    fetch_end   = datetime(dt_end.year, dt_end.month, dt_end.day, tzinfo=timezone.utc) + timedelta(days=1)

    print(f"\n[STEP1] Binance XRP/USDT 日足取得...")
    klines = fetch_klines(SYMBOL, INTERVAL, fetch_start, fetch_end)
    if not klines:
        print("[ERROR] Binance からデータが取得できません")
        return

    print(f"\n[STEP2] USD/JPY レート取得...")
    fx_rates = fetch_usd_jpy_daily(dt_start, dt_end)
    if not fx_rates:
        print("[WARN] 為替レートが取得できません（USDのみで続行）")

    # Forward-fill: 欠損日を前日のレートで補填
    cache_fx = {}
    last_rate = None
    cur = dt_start
    while cur <= dt_end:
        key = cur.strftime("%Y-%m-%d")
        if key in fx_rates:
            last_rate = fx_rates[key]
        if last_rate is not None:
            cache_fx[key] = last_rate
        cur += timedelta(days=1)

    cache = {}
    for k in klines:
        open_time_ms = k[0]
        close_price  = float(k[4])
        open_dt = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
        date_key = open_dt.strftime("%Y-%m-%d")

        if not (dt_start.date() <= open_dt.date() <= dt_end.date()):
            continue

        usd_close = close_price
        usd_jpy = cache_fx.get(date_key)
        if usd_jpy is None:
            usd_jpy = 1.0  # デフォルト（またはスキップ）
            print(f"[WARN] {date_key} の為替レートが無いため 1.0 を使用")

        jpy_close = usd_close * usd_jpy
        cache[date_key] = [round(usd_close, 6), round(jpy_close, 2)]

    os.makedirs("cache", exist_ok=True)
    path = os.path.join(
        "cache",
        f"xrp_price_close_usd_jpy_{dt_start.date()}_{dt_end.date()}.json"
    )

    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, separators=(",", ":"))

    print(f"\n[OK] {path} に保存しました ({len(cache)} 日分)")
    return cache


if __name__ == "__main__":
    dt_start = datetime(2022, 10, 1, tzinfo=timezone.utc)
    dt_end   = datetime(2025, 12, 1, tzinfo=timezone.utc)

    build_price_cache(dt_start, dt_end)