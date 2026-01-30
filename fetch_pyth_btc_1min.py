#!/usr/bin/env python3
"""
Fetch one year of Pyth BTC/USD 1-minute (60s interval) historical data.

Uses Pyth Benchmarks Updates API (price feed ID):
  GET /v1/updates/price/{timestamp}/{interval}
  https://benchmarks.pyth.network/docs#/Updates/price_updates_interval_route_v1_updates_price__timestamp___interval__get

Price feed ID: 0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43 (BTC/USD)
Interval: 60 seconds (max allowed). One API call per minute → 525,600 calls for 1 year.
"""

import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    requests = None

BASE_URL = "https://benchmarks.pyth.network/v1/updates/price"
# BTC/USD price feed ID (with or without 0x)
FEED_ID_RAW = "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"
INTERVAL_SEC = 60
SECONDS_PER_DAY = 86400
MINUTES_PER_YEAR = 365 * 24 * 60  # 525,600
# Benchmarks API: 90 requests per 10 seconds (TradingView endpoint; assume same for Updates).
# 10/90 ≈ 0.111 s between requests to stay under limit; use 0.12 s for safety.
DEFAULT_DELAY = 0.12


def normalize_feed_id(s: str) -> str:
    """Return hex id without 0x for API."""
    return s.lower().removeprefix("0x")


def fetch_window(timestamp: int, interval: int = INTERVAL_SEC, feed_id: str | None = None) -> list:
    """Request price updates for one 60s window. Returns list of parsed feed updates for our id."""
    fid = normalize_feed_id(feed_id or FEED_ID_RAW)
    url = f"{BASE_URL}/{timestamp}/{interval}"
    params = {"ids": fid, "parsed": "true"}
    if requests:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
    else:
        import urllib.request
        import urllib.parse
        qs = urllib.parse.urlencode(params)
        with urllib.request.urlopen(f"{url}?{qs}", timeout=30) as resp:
            data = json.loads(resp.read().decode())

    if not isinstance(data, list):
        return []
    rows = []
    for item in data:
        parsed = item.get("parsed") or []
        if isinstance(parsed, dict):
            parsed = [parsed]
        for p in parsed:
            if p.get("id", "").lower().replace("0x", "") != fid:
                continue
            price_obj = p.get("price") or {}
            raw_price = price_obj.get("price")
            expo = price_obj.get("expo", -8)
            publish_time = price_obj.get("publish_time")
            conf = price_obj.get("conf", "0")
            if raw_price is not None and publish_time is not None:
                try:
                    actual = int(raw_price) * (10 ** expo)
                    conf_val = int(conf) * (10 ** expo) if conf else None
                except (TypeError, ValueError):
                    actual = None
                    conf_val = None
                rows.append({
                    "timestamp": publish_time,
                    "datetime_utc": datetime.fromtimestamp(publish_time, tz=timezone.utc).isoformat(),
                    "price": actual,
                    "conf": conf_val,
                    "expo": expo,
                })
    return rows


def one_price_per_minute(rows: list) -> dict | None:
    """Pick one price per minute (e.g. first in window). Returns single row dict or None."""
    if not rows:
        return None
    # Use first update in the window (or could use last)
    r = rows[0]
    return {
        "timestamp": r["timestamp"],
        "datetime_utc": r["datetime_utc"],
        "price": r["price"],
        "conf": r["conf"],
    }


def run(
    feed_id: str = FEED_ID_RAW,
    out_dir: Path | None = None,
    delay: float = DEFAULT_DELAY,
    resume_from_ts: int | None = None,
    max_minutes: int | None = None,
) -> tuple[Path, Path]:
    """Fetch 1 year of 1-min data, append to CSV/JSON. Returns (csv_path, json_path)."""
    out_dir = out_dir or Path(__file__).resolve().parent
    csv_path = out_dir / "pyth_btc_usd_1min_1y.csv"
    json_path = out_dir / "pyth_btc_usd_1min_1y.json"
    checkpoint_path = out_dir / "pyth_btc_1min_checkpoint.txt"

    now = int(time.time())
    end_ts = now
    start_ts = end_ts - (365 * SECONDS_PER_DAY)
    # Align to minute boundaries
    start_ts = (start_ts // INTERVAL_SEC) * INTERVAL_SEC
    end_ts = (end_ts // INTERVAL_SEC) * INTERVAL_SEC

    if resume_from_ts is not None:
        start_ts = max(start_ts, resume_from_ts)
    total_minutes = (end_ts - start_ts) // INTERVAL_SEC
    if max_minutes is not None:
        total_minutes = min(total_minutes, max_minutes)

    existing = []
    if csv_path.exists():
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            existing = list(reader)
    if existing and resume_from_ts is None:
        last_ts = max(int(r["timestamp"]) for r in existing)
        start_ts = max(start_ts, last_ts + INTERVAL_SEC)

    all_rows = list(existing)
    t = start_ts
    fetched = 0
    fid = normalize_feed_id(feed_id)

    def flush_files(rows: list[dict]) -> None:
        """Write current rows to CSV and JSON (continuous save)."""
        if not rows:
            return
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["timestamp", "datetime_utc", "price", "conf"])
            w.writeheader()
            w.writerows(rows)
        with open(json_path, "w") as f:
            json.dump(rows, f, indent=2)

    try:
        while t < end_ts and (max_minutes is None or fetched < max_minutes):
            try:
                rows = fetch_window(t, INTERVAL_SEC, feed_id)
                row = one_price_per_minute(rows)
                if row is not None:
                    all_rows.append(row)
                fetched += 1
            except Exception as e:
                print(f"Error at t={t}: {e}", file=sys.stderr)
            t += INTERVAL_SEC
            if delay > 0:
                time.sleep(delay)
            if fetched % 100 == 0 and fetched:  # checkpoint + flush every 100 points
                with open(checkpoint_path, "w") as f:
                    f.write(str(t))
                flush_files(all_rows)  # save continuously every 100 minutes
                print(f"Progress: {fetched} minutes, last ts={t - INTERVAL_SEC}")
    finally:
        if all_rows:
            flush_files(all_rows)
        if checkpoint_path.exists():
            checkpoint_path.unlink()

    return csv_path, json_path


def main() -> None:
    out_dir = Path(__file__).resolve().parent
    delay = DEFAULT_DELAY
    max_minutes = None  # full year
    resume_ts = None

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--delay" and i + 1 < len(args):
            delay = float(args[i + 1])
            i += 2
            continue
        if a == "--max-minutes" and i + 1 < len(args):
            max_minutes = int(args[i + 1])
            i += 2
            continue
        if a == "--resume":
            checkpoint = out_dir / "pyth_btc_1min_checkpoint.txt"
            if checkpoint.exists():
                resume_ts = int(checkpoint.read_text().strip())
            i += 1
            continue
        if a == "--out-dir" and i + 1 < len(args):
            out_dir = Path(args[i + 1])
            i += 2
            continue
        i += 1

    print("Fetching 1 year of BTC/USD 1-minute data (60s interval)...")
    print(f"Feed ID: {FEED_ID_RAW}, delay={delay}s")
    if max_minutes:
        print(f"Limiting to {max_minutes} minutes (use no --max-minutes for full year)")
    csv_path, json_path = run(out_dir=out_dir, delay=delay, resume_from_ts=resume_ts, max_minutes=max_minutes)
    print(f"Saved CSV: {csv_path}")
    print(f"Saved JSON: {json_path}")


if __name__ == "__main__":
    main()
