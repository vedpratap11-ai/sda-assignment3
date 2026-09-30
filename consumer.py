import json
import os
import statistics
from collections import defaultdict, deque
from datetime import datetime, timezone

import certifi
from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import ASCENDING, MongoClient

load_dotenv()
print("MONGO_URI is:", os.getenv("MONGO_URI"))

TOPIC = "crypto.prices"
WINDOW = 20            # ticks kept for rolling volatility
Z_THRESHOLD = 2.5      # abnormal if move > 2.5 std devs
HARD_THRESHOLD = 0.5   # or any single-tick move above 0.5%

client = MongoClient(os.getenv("MONGO_URI"), tlsCAFile=certifi.where())
col = client[os.getenv("MONGO_DB", "crypto_stream")][os.getenv("MONGO_COLLECTION", "prices")]
col.create_index([("coin", ASCENDING), ("ts", ASCENDING)])

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    group_id="crypto-analytics",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

last_price = {}
returns = defaultdict(lambda: deque(maxlen=WINDOW))


def enrich(ev):
    coin, price = ev["coin"], ev["price"]
    ret = 0.0
    if coin in last_price and last_price[coin]:
        ret = (price - last_price[coin]) / last_price[coin] * 100
    last_price[coin] = price

    hist = list(returns[coin])
    vol = statistics.pstdev(hist) if len(hist) >= 3 else 0.0

    alert = None
    if len(hist) >= 5 and vol > 0 and abs(ret) > Z_THRESHOLD * vol:
        alert = "z-score spike"
    elif abs(ret) >= HARD_THRESHOLD:
        alert = "large move"

    returns[coin].append(ret)

    ev.update({
        "ts": datetime.now(timezone.utc),
        "tick_return_pct": round(ret, 4),
        "volatility": round(vol, 4),
        "alert": alert,
    })
    return ev


def main():
    print(f"Consuming '{TOPIC}' -> MongoDB Atlas. Ctrl+C to stop.")
    for msg in consumer:
        doc = enrich(msg.value)
        col.insert_one(doc)
        flag = f"  ALERT: {doc['alert']}" if doc["alert"] else ""
        print(f"{doc['symbol']} ${doc['price']} ret={doc['tick_return_pct']}% vol={doc['volatility']}{flag}")


if __name__ == "__main__":
    main()