# Real-Time Crypto Risk Dashboard

A streaming data pipeline that ingests live cryptocurrency prices, computes
volatility and anomaly signals in real time, and displays the results on a
live dashboard.

## Pipeline

**Why this topic:** Streaming analytics is especially valuable in
Banking/Fintech because crypto and forex prices can move within seconds.
Delayed, batch-based reporting is not useful for managing risk — this
pipeline detects abnormal volatility and large price swings as they happen,
so a manager could act before losses grow.

## Components

- **producer.py** — polls the Binance public REST API every 15 seconds for
  BTC, ETH, SOL, XRP and ADA, and publishes each price as a JSON event to
  the Kafka topic `crypto.prices`.
- **consumer.py** — subscribes to `crypto.prices`, computes a tick-to-tick
  percentage return and rolling volatility (standard deviation) per coin,
  flags abnormal moves as alerts (z-score spikes or large single-tick
  moves), and stores each enriched document in MongoDB Atlas.
- **dashboard.py** — a Flask app that queries Atlas and serves the data via
  REST endpoints (`/api/history`, `/api/summary`, `/api/alerts`), rendered
  live in the browser with Chart.js. Refreshes every 10 seconds.

## Dashboard charts

1. **Price trend** (line chart) — live price for a selected coin over the
   last 30/60/360 minutes.
2. **24h change by coin** (bar chart) — percentage change across all coins.
3. **Current rolling volatility** (bar chart) — standard deviation of
   recent tick returns per coin.
4. **Latest volatility alerts** (table) — most recent abnormal price moves,
   updated live.

2. Copy `.env.example` to `.env` and fill in your own values:
3. Start a local Kafka broker (e.g. via Docker), or point
   `KAFKA_BOOTSTRAP` at your own broker.
4. In three separate terminals, from the project root, run in this order:
  
5. Open **http://localhost:5000** in your browser.

## Notes

- Binance's public market-data endpoint requires no API key.
- Let the pipeline run for 10–15 minutes before taking screenshots so the
  charts and alert table have meaningful data.
## Setup

1. Clone this repo and install dependencies:
