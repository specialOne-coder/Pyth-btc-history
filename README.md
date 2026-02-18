# Pyth BTC/USD 1-Year Historical Data

Three ways to get Pyth BTC/USD data from [Pyth Benchmarks](https://benchmarks.pyth.network):

1. **Daily bars (1 request)** – `fetch_pyth_btc_history.py` → TradingView History API  
2. **1-minute interval (525,600 requests)** – `fetch_pyth_btc_1min.py` → Updates API with price feed ID
3. **1-second interval (~31.5M points, 525,600 requests)** – `fetch_pyth_btc_1sec.py` → Updates API 60s windows, saves to DB + CSV/JSON

---

## 1. Daily bars (TradingView History API)

- **Endpoint:** `GET https://benchmarks.pyth.network/v1/shims/tradingview/history`
- **Docs:** [Pyth historical price data](https://docs.pyth.network/price-feeds/core/use-historical-price-data), [Benchmarks API](https://benchmarks.pyth.network/docs#/TradingView/history_v1_shims_tradingview_history_get)
- **Symbol:** `Crypto.BTC/USD`
- **Resolutions:** `1`, `2`, `5`, `15`, `30`, `60`, `120`, `240`, `360`, `720` (minutes), `D`/`1D`, `W`/`1W`, `M`/`1M`

**Rate limit (TradingView endpoint):** 90 requests every 10 seconds. Requests above that limit receive **429 (Too Many Requests)** for the **subsequent 60-second period**.

## Setup

No install required: the script uses Python’s built-in `urllib` by default. For a nicer HTTP client you can use `requests`:

```bash
cd pyth-btc-history
pip install -r requirements.txt   # optional; or use a venv
```

## Run

```bash
python fetch_pyth_btc_history.py
```

Output files in the same directory:

- `pyth_btc_usd_1y.csv` – OHLCV + timestamp and UTC datetime
- `pyth_btc_usd_1y.json` – same data as JSON

Optional arguments:

```bash
python fetch_pyth_btc_history.py [resolution] [output.csv] [output.json]
```

Example (daily bars, custom paths):

```bash
python fetch_pyth_btc_history.py D ./out/btc_daily.csv ./out/btc_daily.json
```

Example (1-hour bars for 1 year; may require multiple requests if the API limits range):

```bash
python fetch_pyth_btc_history.py 60
```

## 2. 1-minute interval (Updates API, price feed ID)

Full year = one data point every **60 seconds** → 525,600 API calls. The script assumes the same rate limit as the TradingView endpoint (**90 req / 10 s**; 429 for 60 s if exceeded) and uses a default delay of 0.12 s.

- **Endpoint:** `GET https://benchmarks.pyth.network/v1/updates/price/{timestamp}/{interval}`
- **Price feed ID:** `0xe62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43` (BTC/USD)
- **Interval:** 60 seconds (max allowed)
- **Docs:** [Benchmarks API – price updates interval](https://benchmarks.pyth.network/docs#/Updates/price_updates_interval_route_v1_updates_price__timestamp___interval__get)

### Run 1-min fetcher

```bash
python fetch_pyth_btc_1min.py
```

Output files: `pyth_btc_usd_1min_1y.csv`, `pyth_btc_usd_1min_1y.json`

Options:

- `--delay 0.12` – seconds between requests (default 0.12 to stay under 90 req/10s; increase if you get 429)
- `--max-minutes N` – fetch only N minutes (e.g. `--max-minutes 100` for testing)
- `--resume` – resume from last checkpoint after interrupt
- `--out-dir DIR` – output directory

Example (test with 5 minutes):

```bash
python fetch_pyth_btc_1min.py --max-minutes 5 --delay 0.05
```

**Rate limit:** The **TradingView endpoint** allows **90 requests every 10 seconds**. Clients above that limit get **429 (Too Many Requests)** for the **next 60 seconds**. The 1-min script uses the Updates API; the same limit is assumed, so the default delay is set to stay under 90/10s.

Full year at default delay (0.12 s) ≈ **17.5 hours**. Use `--resume` if interrupted.

### Time estimate (API limitation: 90 req / 10 s)

To stay under **90 requests per 10 seconds**, use at least ~0.112 s between requests (default **0.12 s**):

| Scenario | Requests | Delay | Approx. time |
|----------|----------|--------|--------------|
| Full 1 year (1-min data) | 525,600 | **0.12 s** (default) | **~17.5 hours** |
| Full 1 year | 525,600 | 0.2 s | ~29.2 hours |
| Full 1 year | 525,600 | 1 s | ~6.1 days |

If you get **429** responses, you are throttled for **60 seconds**; increase `--delay` and run again with `--resume`.

### Output format (1-min data)

Two files are written in the same directory:

- **CSV:** `pyth_btc_usd_1min_1y.csv` (comma-separated, header row)
- **JSON:** `pyth_btc_usd_1min_1y.json` (array of objects)

**Columns:**

| Column       | Type    | Description |
|-------------|---------|-------------|
| `timestamp` | integer | Unix time (seconds UTC) |
| `datetime_utc` | string | ISO 8601 UTC, e.g. `2025-01-29T16:51:00+00:00` |
| `price`     | number  | Pyth price (decimal, e.g. 102071.60) |
| `conf`      | number  | Confidence interval (same scale as price) |

**CSV example:**

```csv
timestamp,datetime_utc,price,conf
1738169460,2025-01-29T16:51:00+00:00,102071.60250813,44.20250813
1738169520,2025-01-29T16:52:00+00:00,102037.70348327,40.50348327
```

**JSON example:**

```json
[
  { "timestamp": 1738169460, "datetime_utc": "2025-01-29T16:51:00+00:00", "price": 102071.60250813, "conf": 44.20250813 },
  { "timestamp": 1738169520, "datetime_utc": "2025-01-29T16:52:00+00:00", "price": 102037.70348327, "conf": 40.50348327 }
]
```

---

## 3. 1-second interval (Updates API + PostgreSQL)

Uses 60-second windows (one request = 61 points, one per second). Saves to **PostgreSQL only** (table `pyth_btc_usd_1sec`).

- **Table:** `pyth_btc_usd_1sec` (timestamp, datetime_utc, price, conf)
- **Rate limit:** 30 req/10s → default delay 0.35s, ~48h for 1 year
- **Database:** Set `DATABASE_URL` (env or `Backend/.env` or `--database-url`). The `?schema=public` param is stripped automatically for psycopg2.

### Run 1-sec fetcher

```bash
pip install -r requirements.txt   # psycopg2-binary, python-dotenv
export DATABASE_URL="postgresql://postgres:postgres_123_ultrastrong_password@145.239.86.18:5432/chartwin?schema=public"
python fetch_pyth_btc_1sec.py
```

Or copy `Backend/.env` or create `pyth-btc-history/.env` with `DATABASE_URL`.

Options:

- `--delay 0.35` – seconds between requests (30 req/10s)
- `--max-minutes N` – fetch only N minutes (e.g. `--max-minutes 60` for testing)
- `--resume` – resume from checkpoint or DB
- `--database-url URL` – override DATABASE_URL
- `--start-days-ago N` – start from N days ago (default 365; use 1 for recent data if 1y ago returns empty)

Example (test 5 minutes):

```bash
python fetch_pyth_btc_1sec.py --max-minutes 5
```

Example (test with recent data; use if 1y ago returns empty due to Benchmarks retention):

```bash
python fetch_pyth_btc_1sec.py --start-days-ago 1 --max-minutes 10
```

---

## Output columns

### Daily (History API)

| Column        | Description                    |
|---------------|--------------------------------|
| timestamp     | Unix time (seconds)            |
| datetime_utc  | ISO UTC datetime               |
| open          | Open price                     |
| high          | High price                     |
| low           | Low price                      |
| close         | Close price                    |
| volume        | Volume (if available)          |

### 1-minute (Updates API)

| Column        | Description                    |
|---------------|--------------------------------|
| timestamp     | Unix time (seconds)            |
| datetime_utc  | ISO UTC datetime               |
| price         | Pyth price (price × 10^expo)   |
| conf          | Confidence (conf × 10^expo)    |
