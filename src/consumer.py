import json
from datetime import datetime, timezone

from kafka import KafkaConsumer

from config import settings
from db import insert_alerts, insert_cleaned, insert_raw
from processing import clean_and_validate, detect_anomalies


def main():
    consumer = KafkaConsumer(
        settings.kafka_topic_raw,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        auto_offset_reset="earliest",
        group_id=settings.kafka_consumer_group,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    previous = None
    last_seen_at = None

    for msg in consumer:
        payload = msg.value
        insert_raw(payload)

        cleaned, notes, is_valid = clean_and_validate(payload, previous)
        alerts = detect_anomalies(cleaned, previous, last_seen_at)

        insert_cleaned(cleaned, notes, alerts, is_valid)
        insert_alerts(cleaned, alerts)

        try:
            last_seen_at = datetime.fromisoformat(cleaned["timestamp"].replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            last_seen_at = datetime.now(timezone.utc)

        previous = cleaned
        print(f"Processed spacecraft={cleaned.get('spacecraft_id')} alerts={alerts} notes={notes}")


if __name__ == "__main__":
    main()
