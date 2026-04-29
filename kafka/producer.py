"""
Kafka Producer: Simulates real-time UHI sensor data streaming.
Reads MODIS CSVs and publishes records to 'uhi-data' Kafka topic.
"""

import csv
import json
import time
import glob
import sys
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "uhi-data"
DELAY = 0.01  # seconds between messages (configurable)

def create_producer():
    """Create Kafka producer with JSON serializer."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3
        )
        print(f"✅ Connected to Kafka broker at {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        sys.exit(1)


def stream_csv_data(producer, csv_files):
    """Read CSVs and stream records to Kafka topic."""
    total_sent = 0
    skipped = 0

    for csv_file in sorted(csv_files):
        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Skip rows with empty LST values
                if not row.get("LST_Day_Urban") or row["LST_Day_Urban"].strip() == "":
                    skipped += 1
                    continue

                # Build message payload
                message = {
                    "city": row["city"],
                    "date": row["date"],
                    "LST_Day_Urban": float(row["LST_Day_Urban"]),
                    "LST_Day_Rural": float(row["LST_Day_Rural"]),
                    "LST_Night_Urban": float(row["LST_Night_Urban"]),
                    "UHI_Index": float(row["UHI_Index"]),
                    "timestamp": time.time()
                }

                # Use city as key for partitioning
                producer.send(
                    TOPIC,
                    key=row["city"],
                    value=message
                )
                total_sent += 1

                if total_sent % 500 == 0:
                    print(f"  📤 Sent {total_sent} records...")

                time.sleep(DELAY)

    producer.flush()
    print(f"\n✅ Streaming complete!")
    print(f"   Total sent: {total_sent}")
    print(f"   Skipped (empty): {skipped}")


def main():
    print("=" * 60)
    print("KAFKA PRODUCER: UHI Data Streaming")
    print("=" * 60)

    csv_files = sorted(glob.glob("data/modis/*.csv"))
    if not csv_files:
        print("❌ No CSV files found in data/modis/")
        sys.exit(1)

    print(f"Found {len(csv_files)} CSV files:")
    for f in csv_files:
        print(f"  - {f}")

    producer = create_producer()
    print(f"\nStreaming to topic: {TOPIC}")
    print(f"Delay between messages: {DELAY}s\n")

    stream_csv_data(producer, csv_files)
    producer.close()


if __name__ == "__main__":
    main()
