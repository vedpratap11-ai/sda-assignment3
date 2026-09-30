import os
from datetime import datetime, timedelta, timezone

import certifi
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from pymongo import MongoClient

load_dotenv()

app = Flask(__name__)
client = MongoClient(os.getenv("MONGO_URI"), tlsCAFile=certifi.where())
col = client[os.getenv("MONGO_DB", "crypto_stream")][os.getenv("MONGO_COLLECTION", "prices")]


def latest_per_coin():
    pipeline = [
        {"$sort": {"ts": -1}},
        {"$group": {"_id": "$coin", "doc": {"$first": "$$ROOT"}}},
    ]
    return [r["doc"] for r in col.aggregate(pipeline)]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/coins")
def coins():
    return jsonify(sorted(col.distinct("coin")))


@app.route("/api/history")
def history():
    coin = request.args.get("coin", "bitcoin")
    minutes = int(request.args.get("minutes", 60))
    since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    rows = col.find(
        {"coin": coin, "ts": {"$gte": since}},
        {"_id": 0, "ts": 1, "price": 1, "volatility": 1},
    ).sort("ts", 1)
    return jsonify([
        {"ts": r["ts"].replace(tzinfo=timezone.utc).isoformat(), "price": r["price"], "volatility": r["volatility"]}
        for r in rows
    ])


@app.route("/api/summary")
def summary():
    docs = latest_per_coin()
    return jsonify([
        {
            "symbol": d["symbol"],
            "price": d["price"],
            "change_24h_pct": d.get("change_24h_pct") or 0,
            "volatility": d["volatility"],
            "volume_24h": d["volume_24h"],
        }
        for d in sorted(docs, key=lambda x: x["symbol"])
    ])


@app.route("/api/alerts")
def alerts():
    rows = col.find({"alert": {"$ne": None}}, {"_id": 0}).sort("ts", -1).limit(10)
    return jsonify([
        {
            "ts": r["ts"].replace(tzinfo=timezone.utc).isoformat(),
            "symbol": r["symbol"],
            "price": r["price"],
            "ret": r["tick_return_pct"],
            "type": r["alert"],
        }
        for r in rows
    ])


if __name__ == "__main__":
    app.run(debug=True, port=5000)
