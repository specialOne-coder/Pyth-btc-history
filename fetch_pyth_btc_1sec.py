#!/usr/bin/env python3
"""
Fetch one year of Pyth BTC/USD 1-second historical data.

Uses 60-second windows (one request = 61 data points, one per second).
Saves to PostgreSQL only (table pyth_btc_usd_1sec).

API: GET /v1/updates/price/{timestamp}/60
Rate: 30 req/10s → delay ~0.35s. ~48h for 1 year.

Database: Set DATABASE_URL (env, .env, or Backend/.env).
Example: postgresql://postgres:password@host:5432/chartwin
(Note: ?schema=public is stripped automatically for psycopg2)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    requests = None

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    psycopg2 = None

BASE_URL = "https://benchmarks.pyth.network/v1/updates/price"
FEED_ID_RAW = "0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43"
INTERVAL_SEC = 60
SECONDS_PER_DAY = 86400
DEFAULT_DELAY = 0.35  # 30 req/10s → ~2.86 req/s, 0.35s between calls

TABLE_NAME = "pyth_btc_usd_1sec"


def load_database_url() -> str | None:
    """Load DATABASE_URL from env, pyth-btc-history/.env, or Backend/.env"""
    url = os.environ.get("DATABASE_URL")
    if url:
        return _fix_dsn_for_psycopg2(url)
    try:
        from dotenv import load_dotenv
        base = Path(__file__).resolve().parent
        for p in (base / ".env", base.parent / "Backend" / ".env"):
            if p.exists():
                load_dotenv(p)
                url = os.environ.get("DATABASE_URL")
                if url:
                    return _fix_dsn_for_psycopg2(url)
    except ImportError:
        pass
    return None


def _fix_dsn_for_psycopg2(url: str) -> str:
    """Remove ?schema=public etc. - psycopg2 doesn't support Prisma query params"""
    from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
    parsed = urlparse(url)
    if not parsed.query:
        return url
    params = parse_qs(parsed.query)
    params.pop("schema", None)  # Prisma uses this, psycopg2 doesn't
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def normalize_feed_id(s: str) -> str:
    return s.lower().removeprefix("0x")


def fetch_window(timestamp: int, interval: int = INTERVAL_SEC, feed_id: str | None = None) -> list:
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
        with urllib.request.urlopen(f"{url}?{urllib.parse.urlencode(params)}", timeout=30) as resp:
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
            if raw_price is None or publish_time is None:
                continue
            try:
                actual = int(raw_price) * (10 ** expo)
                conf_val = int(conf) * (10 ** expo) if conf else None
            except (TypeError, ValueError):
                continue
            rows.append({
                "timestamp": publish_time,
                "datetime_utc": datetime.fromtimestamp(publish_time, tz=timezone.utc).isoformat(),
                "price": actual,
                "conf": conf_val,
            })
    return rows


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


def ensure_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                timestamp BIGINT PRIMARY KEY,
                datetime_utc TIMESTAMPTZ NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                conf DOUBLE PRECISION
            )
        """)
        conn.commit()


def get_last_timestamp_from_db(conn) -> int | None:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(timestamp) FROM {TABLE_NAME}")
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else None


def insert_rows_db(conn, rows: list) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        values = [(r["timestamp"], r["datetime_utc"], r["price"], r.get("conf")) for r in rows]
        execute_values(
            cur,
            f"INSERT INTO {TABLE_NAME} (timestamp, datetime_utc, price, conf) VALUES %s ON CONFLICT (timestamp) DO NOTHING",
            values,
        )
        conn.commit()
        return len(rows)


def run(
    feed_id: str = FEED_ID_RAW,
    out_dir: Path | None = None,
    delay: float = DEFAULT_DELAY,
    resume_from_ts: int | None = None,
    max_minutes: int | None = None,
    start_days_ago: int | None = None,
    database_url: str | None = None,
) -> None:
    out_dir = out_dir or Path(__file__).resolve().parent
    checkpoint_path = out_dir / "pyth_btc_1sec_checkpoint.txt"

    now = int(time.time())
    end_ts = (now // INTERVAL_SEC) * INTERVAL_SEC
    days_back = 365 if start_days_ago is None else start_days_ago
    start_ts = ((now - days_back * SECONDS_PER_DAY) // INTERVAL_SEC) * INTERVAL_SEC
    if resume_from_ts is not None:
        start_ts = max(start_ts, resume_from_ts)
    if max_minutes is not None:
        end_ts = min(end_ts, start_ts + max_minutes * INTERVAL_SEC)

    # DB connection (required)
    if not psycopg2:
        _log("ERROR: psycopg2 required. pip install psycopg2-binary")
        sys.exit(1)
    raw_url = database_url or load_database_url()
    if not raw_url:
        _log("ERROR: DATABASE_URL not set (env, .env, or Backend/.env)")
        sys.exit(1)
    db_url = _fix_dsn_for_psycopg2(raw_url)
    try:
        conn = psycopg2.connect(db_url)
    except Exception as e:
        _log(f"ERROR: cannot connect to DB: {e}")
        sys.exit(1)
    ensure_table(conn)
    db_last = get_last_timestamp_from_db(conn)
    if db_last is not None and resume_from_ts is None:
        start_ts = max(start_ts, db_last + 1)
        _log(f"Resuming from DB: last ts={db_last}, next from {start_ts}")

    total_inserted = 0
    t = start_ts
    fetched_windows = 0
    total_windows = max(0, (end_ts - start_ts) // INTERVAL_SEC)
    start_time = time.monotonic()

    _log(f"Start: {total_windows} windows (~{total_windows * 61} points) from {datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} to {datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC")
    _log("Saving to database only")

    try:
        while t < end_ts and (max_minutes is None or fetched_windows < max_minutes):
            try:
                rows = fetch_window(t, INTERVAL_SEC, feed_id)
                if rows:
                    insert_rows_db(conn, rows)
                    total_inserted += len(rows)
                fetched_windows += 1
            except Exception as e:
                _log(f"ERROR t={t}: {e}")
            t += INTERVAL_SEC
            if delay > 0:
                time.sleep(delay)
            if fetched_windows > 0 and fetched_windows % 100 == 0:
                with open(checkpoint_path, "w") as f:
                    f.write(str(t))
                elapsed = time.monotonic() - start_time
                pct = 100.0 * fetched_windows / total_windows if total_windows else 0
                eta_sec = (elapsed / fetched_windows) * (total_windows - fetched_windows) if fetched_windows < total_windows else 0
                eta_str = f", ETA ~{int(eta_sec // 3600)}h{int((eta_sec % 3600) // 60)}m" if fetched_windows < total_windows else ""
                _log(f"Progress: {fetched_windows}/{total_windows} windows ({total_inserted} points, {pct:.1f}%){eta_str}")
    finally:
        conn.close()
        if checkpoint_path.exists():
            checkpoint_path.unlink()
        elapsed = time.monotonic() - start_time
        _log(f"Done: {total_inserted} points inserted, {elapsed / 3600:.1f}h elapsed")


def main() -> None:
    out_dir = Path(__file__).resolve().parent
    delay = DEFAULT_DELAY
    max_minutes = None
    resume_ts = None
    start_days_ago = None
    database_url = None
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--delay" and i + 1 < len(args):
            delay = float(args[i + 1])
            i += 2
        elif a == "--max-minutes" and i + 1 < len(args):
            max_minutes = int(args[i + 1])
            i += 2
        elif a == "--resume":
            cp = out_dir / "pyth_btc_1sec_checkpoint.txt"
            if cp.exists():
                resume_ts = int(cp.read_text().strip())
            i += 1
        elif a == "--out-dir" and i + 1 < len(args):
            out_dir = Path(args[i + 1])
            i += 2
        elif a == "--database-url" and i + 1 < len(args):
            database_url = args[i + 1]
            i += 2
        elif a == "--start-days-ago" and i + 1 < len(args):
            start_days_ago = int(args[i + 1])
            i += 2
        else:
            i += 1

    _log("Fetching 1y BTC/USD 1-sec (60s windows, 61 points/window)")
    _log(f"Delay={delay}s")
    run(
        out_dir=out_dir,
        delay=delay,
        resume_from_ts=resume_ts,
        max_minutes=max_minutes,
        start_days_ago=start_days_ago,
        database_url=database_url,
    )
    _log(f"Table: {TABLE_NAME}")


if __name__ == "__main__":
    main()
