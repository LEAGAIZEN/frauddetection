"""
producer.py
Streams the Kaggle Credit Card Fraud dataset into a Kafka topic,
replaying the original transaction timing at a scaled-down speed
so downstream Spark windows and the live dashboard see a realistic
arrival pattern instead of a burst of 284K messages at once.

Usage:
    python producer.py --csv ../data/creditcard.csv --speed 200
    (speed=200 means 1 real second of dataset time = 1/200th of a wall-clock second)
"""

import argparse
import json
import time
import sys
import os

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")
DEFAULT_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")


def build_producer(broker: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
    )


def stream(csv_path: str, broker: str, speed: float, limit: int | None):
    df = pd.read_csv(csv_path)
    if limit:
        df = df.head(limit)

    producer = build_producer(broker)
    sent = 0
    prev_time = None

    print(f"Streaming {len(df)} rows to topic '{TOPIC}' at {broker} (speed={speed}x)")

    try:
        for _, row in df.iterrows():
            # Sleep to replay the original gap between transactions, scaled down.
            if prev_time is not None:
                gap_seconds = max(0.0, row["Time"] - prev_time)
                time.sleep(min(gap_seconds / speed, 2.0))  # cap so a big gap doesn't stall the demo
            prev_time = row["Time"]

            message = {col: float(row[col]) for col in row.index if col != "Class"}
            message["Class"] = int(row["Class"])
            producer.send(TOPIC, value=message)
            sent += 1

            if sent % 500 == 0:
                print(f"  sent {sent}/{len(df)}")

        producer.flush()
        print(f"Done. Sent {sent} transactions.")

    except KeyboardInterrupt:
        print(f"\nInterrupted — flushing {sent} sent so far.")
        producer.flush()
        sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="../data/creditcard.csv")
    parser.add_argument("--broker", default=DEFAULT_BROKER)
    parser.add_argument("--speed", type=float, default=200.0,
                         help="Replay speed multiplier — higher = faster than real time")
    parser.add_argument("--limit", type=int, default=None,
                         help="Only stream the first N rows (useful for quick tests)")
    args = parser.parse_args()

    stream(args.csv, args.broker, args.speed, args.limit)