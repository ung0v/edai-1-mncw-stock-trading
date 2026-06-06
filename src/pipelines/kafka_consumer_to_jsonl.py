import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaException

sys.path.append(str(Path(__file__).resolve().parents[2]))


from paths import PROJECT_DIR

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trading_events")
MAX_MESSAGES = int(os.getenv("KAFKA_MAX_MESSAGES", "1000"))

OUTPUT_PATH = (
    PROJECT_DIR / "data" / "raw" / "stream" / "trading_events" / "kafka_events.jsonl"
)


def now_utc():
    return datetime.now(timezone.utc).isoformat()


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "stock-trading-coursework",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )

    consumer.subscribe([KAFKA_TOPIC])

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    consumed = 0

    try:
        with open(OUTPUT_PATH, "w") as f:
            while consumed < MAX_MESSAGES:
                msg = consumer.poll(timeout=5.0)

                if msg is None:
                    print("No more messages available right now.")
                    break

                if msg.error():
                    raise KafkaException(msg.error())

                event = json.loads(msg.value().decode("utf-8"))
                event["kafka_consumed_ts"] = now_utc()
                event["kafka_topic"] = msg.topic()
                event["kafka_partition"] = msg.partition()
                event["kafka_offset"] = msg.offset()

                f.write(json.dumps(event) + "\n")
                consumed += 1

    finally:
        consumer.close()

    print(f"Consumed {consumed} events from topic={KAFKA_TOPIC}")
    print(f"Output path: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
