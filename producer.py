import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

TOPIC = "crypto.prices"

# Binance symbols (USDT pairs). No API key required for public market data.
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]
URL = "https://api.binance.com/api/v3/ticker/24hr"
POLL_SECONDS = 15  # Binance's public endpoints allow frequent polling

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=5,
)


def fetch_prices():
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; CryptoPipeline/1.0)",
        "Accept": "application/json",
    }
    resp = requests.get(URL, headers=headers, timeout=10)
    resp.raise_for_status()
    all_tickers = resp.json()

    # Binance returns data for every symbol; filter to the ones we want
    wanted = set(SYMBOLS)
    return [t for t in all_tickers if t["symbol"] in wanted]


def main():
    print(f"Producing to '{TOPIC}' every {POLL_SECONDS}s. Ctrl+C to stop.")
    while True:
        try:
            for t in fetch_prices():
                symbol = t["symbol"]
                coin_id = symbol.replace("USDT", "").lower()
                event = {
                    "coin": coin_id,
                    "symbol": symbol.replace("USDT", ""),
                    "price": float(t["lastPrice"]),
                    "market_cap": None,  # not provided by this endpoint
                    "volume_24h": float(t["quoteVolume"]),
                    "high_24h": float(t["highPrice"]),
                    "low_24h": float(t["lowPrice"]),
                    "change_24h_pct": float(t["priceChangePercent"]),
                    "change_1h_pct": None,  # not provided by this endpoint
                    "source_ts": datetime.fromtimestamp(
                        t["closeTime"] / 1000, tz=timezone.utc
                    ).isoformat(),
                    "produced_ts": datetime.now(timezone.utc).isoformat(),
                }
                # key = coin id, so each coin stays ordered within a partition
                producer.send(TOPIC, key=coin_id, value=event)
                print(f"sent {event['symbol']}: ${event['price']}")
            producer.flush()
        except requests.RequestException as e:
            print("API error (will retry):", e)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
