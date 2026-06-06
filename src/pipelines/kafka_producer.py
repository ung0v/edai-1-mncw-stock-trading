import json
import os
import sys
from pathlib import Path

from confluent_kafka import Producer

sys.path.append(str(Path(__file__).resolve().parents[2]))

from paths import STREAM_EVENTS_PATH

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trading_events")


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def load_events(path: Path):
    with open(path, "r") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "stock-trading-producer",
        }
    )

    sent = 0

    for event in load_events(STREAM_EVENTS_PATH):
        event_id = event["event_id"]

        producer.produce(
            topic=KAFKA_TOPIC,
            key=event_id,
            value=json.dumps(event),
            callback=delivery_report,
        )

        producer.poll(0)
        sent += 1

    producer.flush()

    print(f"Produced {sent} events to topic={KAFKA_TOPIC}")


if __name__ == "__main__":
    main()
