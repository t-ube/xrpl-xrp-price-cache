"""
GitHub Actions から実行して、
R2 上の xrp_oracle_daily.json を差分更新するスクリプト。

- R2 へのアクセスは Cloudflare API Token (Bearer) で行う
- Binance: XRPUSDT 日足 close
- Frankfurter: USD/JPY 日次（平日）。土日等は直前営業日のレートを継承。
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict

import requests
from urllib.parse import quote

# ========= 環境変数 =========
# Cloudflare
CF_API_TOKEN = os.environ["CLOUDFLARE_API_TOKEN"]
CF_ACCOUNT_ID = os.environ["CLOUDFLARE_ACCOUNT_ID"]

# R2
R2_BUCKET = os.environ["R2_BUCKET"]
R2_OBJECT_KEY = os.environ.get("R2_OBJECT_KEY", "xrp_oracle_daily.json")

# データ開始日（JSON がまだ存在しないときの初期日付）
INITIAL_START_DATE = os.environ.get("INITIAL_START_DATE", "2022-10-01")

# 市場データ
BINANCE_BASE_URL = "https://api.binance.com"
BINANCE_SYMBOL = "XRPUSDT"
BINANCE_INTERVAL = "1d"
BINANCE_LIMIT = 1000

FX_BASE_URL = "https://api.frankfurter.app"


# ========= 日付ユーティリティ =========

def date_str_to_dt(date_str: str) -> datetime:
    return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)


def dt_to_date_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def next_date_str(date_str: str) -> str:
    dt = date_str_to_dt(date_str)
    return dt_to_date_str(dt + timedelta(days=1))


def yesterday_utc_str() -> str:
    now = datetime.now(timezone.utc)
    y = (now - timedelta(days=1)).date()
    return y.strftime("%Y-%m-%d")


def dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


# ========= R2 JSON 読み書き (Cloudflare API) =========

def r2_object_url() -> str:
    key_enc = quote(R2_OBJECT_KEY, safe="")
    return (
        f"https://api.cloudflare.com/client/v4/accounts/"
        f"{CF_ACCOUNT_ID}/r2/buckets/{R2_BUCKET}/objects/{key_enc}"
    )


def load_json_from_r2() -> dict:
    url = r2_object_url()
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
    }
    print(f"[R2] GET {url}")
    res = requests.get(url, headers=headers, timeout=20)

    if res.status_code == 404:
        print(f"[R2] オブジェクトが存在しません。新規作成します: {R2_BUCKET}/{R2_OBJECT_KEY}")
        data = {"meta": {"version": 1, "last_date": None}, "daily": {}}
    else:
        res.raise_for_status()
        data = res.json()
        print(f"[R2] 既存JSONを読み込み: {R2_BUCKET}/{R2_OBJECT_KEY}")

    daily = data.get("daily", {})
    max_date = max(daily.keys()) if daily else None

    if "meta" not in data:
        data["meta"] = {"version": 1, "last_date": max_date}
    else:
        data["meta"].setdefault("version", 1)
        if data["meta"].get("last_date") is None:
            data["meta"]["last_date"] = max_date

    return data


def save_json_to_r2(data: dict) -> None:
    url = r2_object_url()
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}",
        "Content-Type": "application/json",
    }
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    print(f"[R2] PUT {url}")
    res = requests.put(url, headers=headers, data=body.encode("utf-8"), timeout=30)
    res.raise_for_status()
    print(f"[R2] JSONを保存しました: {R2_BUCKET}/{R2_OBJECT_KEY}")


# ========= Binance XRPUSDT 日足 =========

def fetch_xrp_usdt_daily(dt_start: datetime, dt_end: datetime) -> Dict[str, float]:
    url = f"{BINANCE_BASE_URL}/api/v3/klines"

    start_ms = dt_to_ms(dt_start)
    end_ms = dt_to_ms(dt_end)
    all_klines = []

    while True:
        params = {
            "symbol": BINANCE_SYMBOL,
            "interval": BINANCE_INTERVAL,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": BINANCE_LIMIT,
        }
        print("[BINANCE] GET", params)
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()
        klines = res.json()
        if not klines:
            break

        all_klines.extend(klines)

        last_open = klines[-1][0]
        if last_open >= end_ms or len(klines) < BINANCE_LIMIT:
            break

        start_ms = last_open + 1
        time.sleep(0.25)

    daily: Dict[str, float] = {}
    for k in all_klines:
        open_ms = k[0]
        close_price = float(k[4])
        open_dt = datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc)
        date_key = dt_to_date_str(open_dt)
        daily[date_key] = close_price

    print(f"[BINANCE] XRPUSDT 日足件数: {len(daily)}")
    return daily


# ========= Frankfurter USDJPY =========

def fetch_usdjpy_timeseries(dt_start: datetime, dt_end: datetime) -> Dict[str, float]:
    start_str = dt_to_date_str(dt_start)
    end_str = dt_to_date_str(dt_end)
    url = f"{FX_BASE_URL}/{start_str}..{end_str}"
    params = {"from": "USD", "to": "JPY"}

    print("[FX] GET", url, params)
    res = requests.get(url, params=params, timeout=15)
    res.raise_for_status()
    j = res.json()

    out: Dict[str, float] = {}
    for d, row in j.get("rates", {}).items():
        if row.get("JPY") is not None:
            out[d] = float(row["JPY"])

    print(f"[FX] USDJPY 日次件数: {len(out)}")
    return out


# ========= 差分埋め本体 =========

def fill_missing_dates() -> None:
    data = load_json_from_r2()
    daily = data.setdefault("daily", {})

    last_date = data["meta"].get("last_date")
    if last_date:
        start_date = next_date_str(last_date)
    else:
        start_date = INITIAL_START_DATE

    end_date = yesterday_utc_str()

    if start_date > end_date:
        print("[INFO] 差分はありません。")
        return

    print(f"[FILL] 差分埋め: {start_date} 〜 {end_date}")

    dt_start = date_str_to_dt(start_date)
    dt_end = date_str_to_dt(end_date)

    daily_usd = fetch_xrp_usdt_daily(dt_start, dt_end)
    fx = fetch_usdjpy_timeseries(dt_start, dt_end)

    last_rate = None
    added = 0

    cur_dt = dt_start
    while cur_dt.date() <= dt_end.date():
        d = dt_to_date_str(cur_dt)

        usd = daily_usd.get(d)
        if usd is None:
            print(f"[WARN] {d} の XRPUSDT 日足がありません。スキップします。")
            cur_dt += timedelta(days=1)
            continue

        if d in fx:
            last_rate = fx[d]
        elif last_rate is None:
            print(f"[WARN] {d} FXレートなし & 過去レートなし → スキップ")
            cur_dt += timedelta(days=1)
            continue

        jpy = usd * last_rate
        daily[d] = {"usd": usd, "jpy": jpy}
        data["meta"]["last_date"] = d
        added += 1

        print(f"[ADD] {d}: usd={usd}, jpy={jpy}")
        cur_dt += timedelta(days=1)

    save_json_to_r2(data)
    print(f"[DONE] {added} 日追加しました。")


if __name__ == "__main__":
    fill_missing_dates()
