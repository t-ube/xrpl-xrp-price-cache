"""
XRP オラクル日次データの「抜け日」をまとめて埋めるスクリプト

前提 JSON 形式:

{
  "meta": {
    "version": 1,
    "last_date": "YYYY-MM-DD"  # なくてもよい（daily の最大日付から再計算）
  },
  "daily": {
    "YYYY-MM-DD": { "USD": 0.0, "JPY": 0.0 },
    ...
  }
}

動作:
- JSON を読み込み
- last_filled_date の翌日から「昨日(UTC)」までを対象に
  - Binance から XRPUSDT 日足 close を取得
  - Frankfurter から USDJPY を timeseries 取得
  - 前営業日のレートで土日なども埋める
- daily[...] を追加し meta.last_date を更新
- 同じファイルに上書き保存
"""

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict

import requests

# ===== 設定 =====

BINANCE_BASE_URL = "https://api.binance.com"
BINANCE_SYMBOL = "XRPUSDT"
BINANCE_INTERVAL = "1d"
BINANCE_LIMIT = 1000

FRANKFURTER_BASE_URL = "https://api.frankfurter.app"

# 既存JSONのパス
JSON_PATH = "./cache/xrp_oracle_daily.json"

# データをどこから始めるか（完全に空のとき用）
INITIAL_START_DATE = "2022-10-01"  # 必要に応じて変更


# ===== ヘルパ =====

def dt_to_millis(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def date_str_to_dt(date_str: str) -> datetime:
    # "YYYY-MM-DD" → UTC 00:00 の datetime
    return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)


def dt_to_date_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def get_yesterday_utc_date_str() -> str:
    now = datetime.now(timezone.utc)
    y = (now - timedelta(days=1)).date()
    return y.strftime("%Y-%m-%d")


def next_date_str(date_str: str) -> str:
    dt = date_str_to_dt(date_str)
    dt_next = dt + timedelta(days=1)
    return dt_to_date_str(dt_next)


# ===== Binance: XRPUSDT 日足まとめ取得 =====

def fetch_klines(symbol: str, interval: str,
                 dt_start: datetime, dt_end: datetime,
                 limit: int = BINANCE_LIMIT):
    url = f"{BINANCE_BASE_URL}/api/v3/klines"
    start_ms = dt_to_millis(dt_start)
    end_ms = dt_to_millis(dt_end)

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

        last_open_time = klines[-1][0]
        if last_open_time >= end_ms or len(klines) < limit:
            break

        start_ms = last_open_time + 1
        time.sleep(0.2)

    print(f"[BINANCE] 取得件数: {len(all_klines)}")
    return all_klines


def build_xrp_usd_daily(dt_start: datetime, dt_end: datetime) -> Dict[str, float]:
    """
    dt_start〜dt_end(両端含む) の XRPUSDT 日足 close を
    { "YYYY-MM-DD": close_usd } にして返す
    """
    klines = fetch_klines(BINANCE_SYMBOL, BINANCE_INTERVAL, dt_start, dt_end)
    daily: Dict[str, float] = {}

    for k in klines:
        open_time_ms = k[0]
        close_price = float(k[4])

        open_dt = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
        if not (dt_start.date() <= open_dt.date() <= dt_end.date()):
            continue

        date_key = open_dt.strftime("%Y-%m-%d")
        daily[date_key] = close_price

    print(f"[XRP/USDT] 日足件数: {len(daily)}")
    return daily


# ===== Frankfurter: USDJPY timeseries =====

def fetch_usd_jpy_timeseries(dt_start: datetime, dt_end: datetime) -> Dict[str, float]:
    start_str = dt_start.strftime("%Y-%m-%d")
    end_str = dt_end.strftime("%Y-%m-%d")
    url = f"{FRANKFURTER_BASE_URL}/{start_str}..{end_str}"
    params = {"from": "USD", "to": "JPY"}

    print(f"[FX] GET {url} {params}")
    res = requests.get(url, params=params, timeout=10)
    res.raise_for_status()
    data = res.json()

    rates = data.get("rates", {})
    out: Dict[str, float] = {}

    for date_key, row in rates.items():
        JPY = row.get("JPY")
        if JPY is not None:
            out[date_key] = float(JPY)

    print(f"[FX] USD/JPY 日次件数: {len(out)}")
    return out


# ===== 差分埋めロジック =====

def load_oracle_json(path: str) -> dict:
    if not os.path.exists(path):
        print(f"[INFO] JSON が存在しないので新規作成します: {path}")
        return {
            "meta": {
                "version": 1,
                "last_date": None,
            },
            "daily": {}
        }

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 念のため last_date を daily の最大から再計算しておく（不整合対策）
    daily = data.get("daily", {})
    if daily:
        max_date = max(daily.keys())
    else:
        max_date = None

    if data.get("meta") is None:
        data["meta"] = {"version": 1, "last_date": max_date}
    else:
        data["meta"].setdefault("version", 1)
        if data["meta"].get("last_date") is None:
            data["meta"]["last_date"] = max_date

    return data


def save_oracle_json(path: str, data: dict):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    print(f"[OK] 保存完了: {path}")


def fill_missing_days(path: str = JSON_PATH):
    data = load_oracle_json(path)
    daily: Dict[str, dict] = data.get("daily", {})

    # どこから埋め始めるか決定
    last_date = data["meta"].get("last_date")
    if last_date:
        start_date = next_date_str(last_date)
    else:
        start_date = INITIAL_START_DATE

    end_date = get_yesterday_utc_date_str()

    print(f"[RANGE] 差分埋め: {start_date} 〜 {end_date}")

    if start_date > end_date:
        print("[INFO] 埋めるべき日付はありません。")
        return

    dt_start = date_str_to_dt(start_date)
    dt_end = date_str_to_dt(end_date)

    # Binance / FX をまとめて取得
    daily_usd = build_xrp_usd_daily(dt_start, dt_end)
    fx = fetch_usd_jpy_timeseries(dt_start, dt_end)

    # 既存データとマージしながら埋める
    missing_fx = 0
    added_days = 0

    # 直近のFXレート（平日の値）。土日などはこれを引き継ぐ
    last_rate = None

    # 既に先にある古い daily 部分から「過去の last_rate」を引き継ぐ必要は基本ないが、
    # 一応 start_date 以前の最大日付が FX にあれば拾う。
    # （厳密にやるならここでもう一回 FX を取るが、簡略化のため省略）

    # 埋める範囲の全日付を順番に見ていく
    cur_dt = dt_start
    while cur_dt.date() <= dt_end.date():
        d = dt_to_date_str(cur_dt)

        # すでに daily に存在するならスキップ（再実行時の冪等性）
        if d in daily:
            print(f"[SKIP] 既存データあり: {d}")
            # FXの last_rate 更新だけしておく
            if d in fx:
                last_rate = fx[d]
            cur_dt += timedelta(days=1)
            continue

        usd_close = daily_usd.get(d)
        if usd_close is None:
            print(f"[WARN] {d} の XRPUSDT 日足がありません。スキップします。")
            cur_dt += timedelta(days=1)
            continue

        # その日のFXがあれば last_rate を更新、なければ最後の値を引き継ぐ
        if d in fx:
            last_rate = fx[d]
        else:
            if last_rate is None:
                print(f"[WARN] {d} の USD/JPY レートが無く、過去レートも無いのでスキップします。")
                missing_fx += 1
                cur_dt += timedelta(days=1)
                continue
            # last_rate をそのまま使う（＝前営業日のレートを土日等に引き継ぎ）

        jpy_close = usd_close * last_rate
        daily[d] = {"USD": usd_close, "JPY": jpy_close}
        data["meta"]["last_date"] = d
        added_days += 1

        print(f"[ADD] {d}: USD={usd_close}, JPY={jpy_close}")

        # 途中でもこまめに保存しておきたいならここで save_oracle_json
        # save_oracle_json(path, data)

        cur_dt += timedelta(days=1)

    # 最後に一度だけ保存
    data["daily"] = daily
    save_oracle_json(path, data)

    print(f"[SUMMARY] 追加 {added_days} 日, FX欠損 {missing_fx} 日")


if __name__ == "__main__":
    fill_missing_days(JSON_PATH)
