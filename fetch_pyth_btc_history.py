#!/usr/bin/env python3
"""
Fetch one year of Pyth BTC/USD historical data and save to file.

Uses Pyth Benchmarks TradingView History API:
  https://benchmarks.pyth.network/v1/shims/tradingview/history

Resolutions: 1, 2, 5, 15, 30, 60, 120, 240, 360, 720 (minutes), D/1D, W/1W, M/1M
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

BASE_URL = "https://benchmarks.pyth.network/v1/shims/tradingview/history"
SYMBOL = "Crypto.BTC/USD"
# Default: daily bars for last 365 days
DEFAULT_RESOLUTION = "D"
SECONDS_PER_DAY = 86400


def fetch_history(symbol: str = SYMBOL, resolution: str = DEFAULT_RESOLUTION,
                  from_ts: int | None = None, to_ts: int | None = None) -> dict:
    """Request OHLCV history from Pyth Benchmarks API."""
    now = int(time.time())
    if to_ts is None:
        to_ts = now
    if from_ts is None:
        from_ts = to_ts - (365 * SECONDS_PER_DAY)

    params = {
        "symbol": symbol,
        "resolution": resolution,
        "from": from_ts,
        "to": to_ts,
    }
    if requests:
        r = requests.get(BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    # Fallback: urllib
    import urllib.request
    import urllib.parse
    url = BASE_URL + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode())


def parsed_bars(data: dict) -> list[dict]:
    """Convert API response (t, o, h, l, c, v) into list of bar dicts."""
    if data.get("s") != "ok":
        raise ValueError(f"API returned status: {data.get('s')} ({data.get('errmsg', '')})")
    t = data.get("t") or []
    o = data.get("o") or []
    h = data.get("h") or []
    l = data.get("l") or []
    c = data.get("c") or []
    v = data.get("v") or []
    n = len(t)
    return [
        {
            "timestamp": t[i],
            "datetime_utc": datetime.fromtimestamp(t[i], tz=timezone.utc).isoformat(),
            "open": o[i] if i < len(o) else None,
            "high": h[i] if i < len(h) else None,
            "low": l[i] if i < len(l) else None,
            "close": c[i] if i < len(c) else None,
            "volume": v[i] if i < len(v) else None,
        }
        for i in range(n)
    ]


def save_csv(bars: list[dict], path: Path) -> None:
    """Write bars to CSV."""
    if not bars:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(bars[0].keys()))
        w.writeheader()
        w.writerows(bars)


def save_json(bars: list[dict], path: Path) -> None:
    """Write bars to JSON."""
    with open(path, "w") as f:
        json.dump(bars, f, indent=2)


def main() -> None:
    resolution = DEFAULT_RESOLUTION
    out_dir = Path(__file__).resolve().parent
    out_csv = out_dir / "pyth_btc_usd_1y.csv"
    out_json = out_dir / "pyth_btc_usd_1y.json"

    if len(sys.argv) > 1:
        resolution = sys.argv[1]
    if len(sys.argv) > 2:
        out_csv = Path(sys.argv[2])
    if len(sys.argv) > 3:
        out_json = Path(sys.argv[3])

    print(f"Fetching 1 year of {SYMBOL} (resolution={resolution})...")
    data = fetch_history(resolution=resolution)
    bars = parsed_bars(data)
    print(f"Received {len(bars)} bars.")

    save_csv(bars, out_csv)
    print(f"Saved CSV: {out_csv}")
    save_json(bars, out_json)
    print(f"Saved JSON: {out_json}")


if __name__ == "__main__":
    main()
