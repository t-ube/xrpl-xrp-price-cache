"""
GitHub Actions から実行して、
R2 上の xrp_oracle_daily.json を差分更新するスクリプト。

- Kraken: XRPUSDT 日足 close
- Frankfurter: USD/JPY 日次（平日）。土日等は直前営業日のレートを継承。
"""

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict
import requests
import boto3
import os
from botocore.exceptions import ClientError

# ========= 環境変数 =========
R2_ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
R2_ENDPOINT = os.environ["R2_ENDPOINT"]          # https://<ACCOUNT>.r2.cloudflarestorage.com
R2_BUCKET = os.environ["R2_BUCKET"]
R2_OBJECT_KEY = os.environ.get("R2_OBJECT_KEY", "xrp_oracle_daily.json")

# データ開始日（JSON がまだ存在しないときの初期日付）
INITIAL_START_DATE = os.environ.get("INITIAL_START_DATE", "2022-10-01")

# 市場データ
BINANCE_BASE_URL = "https://api2.binance.com"
BINANCE_SYMBOL = "XRPUSDT"
BINANCE_INTERVAL = "1d"
BINANCE_LIMIT = 1000

FX_BASE_URL = "https://api.frankfurter.app"

# ========= R2 クライアント作成 =========
s3 = boto3.client(
    service_name="s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto",
)

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
def load_json_from_r2() -> dict:
    try:
        print(f"[R2] get_object bucket={R2_BUCKET} key={R2_OBJECT_KEY}")
        res = s3.get_object(Bucket=R2_BUCKET, Key=R2_OBJECT_KEY)
        body = res["Body"].read()
        data = json.loads(body.decode("utf-8"))
        print("[R2] 既存JSONを読み込みました")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "NoSuchBucket"):
            print("[R2] オブジェクトがないので新規作成します")
            data = {"meta": {"version": 1, "last_date": None}, "daily": {}}
        else:
            raise

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
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    print(f"[R2] put_object bucket={R2_BUCKET} key={R2_OBJECT_KEY}")
    s3.put_object(Bucket=R2_BUCKET, Key=R2_OBJECT_KEY, Body=body, ContentType="application/json")
    print("[R2] JSON を保存しました")


# ========= Kraken XRPUSDT 日足 =========

def fetch_xrp_usdt_daily(start_dt, end_dt):
    """
    Kraken から日足終値 (close) を取得する。
    return: dict["YYYY-MM-DD"] = float(close)
    """
    since = int(start_dt.timestamp())

    url = "https://api.kraken.com/0/public/OHLC"
    params = {
        "pair": "XRPUSD",   # Krakenは内部で XXRPZUSD に変換する場合あり
        "interval": 1440,   # 1日足
        "since": since,
    }

    print("[KRAKEN] GET", params)
    res = requests.get(url, params=params, timeout=10)
    res.raise_for_status()

    data = res.json()

    # エラーがあったら空で返す
    if data.get("error"):
        print("[KRAKEN] API Error:", data["error"])
        return {}

    # Kraken は "XRPUSD" と指定しても結果キーが "XXRPZUSD" になる
    result_key = None
    for key in data["result"].keys():
        if key.startswith("XXRP") and key.endswith("USD"):
            result_key = key
            break

    if not result_key:
        print("[KRAKEN] Unexpected result keys:", data["result"].keys())
        return {}

    ohlc_list = data["result"][result_key]

    daily = {}
    for entry in ohlc_list:
        ts = entry[0]                # UNIX timestamp (UTC)
        close_price = float(entry[4])  # close
        day = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        daily[day] = close_price

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
