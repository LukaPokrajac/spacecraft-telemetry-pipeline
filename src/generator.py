import json
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

from config import settings


def telemetry_stream(spacecraft_id: str):
    temperature = 22.0
    pressure = 101.3
    fuel_level = 100.0
    battery_voltage = 28.0
    vibration = 0.4
    oxygen_level = 95.0

    while True:
        now = datetime.now(timezone.utc)

        temperature += random.uniform(-0.2, 0.25)
        pressure += random.uniform(-0.15, 0.15)
        fuel_level = max(0.0, fuel_level - random.uniform(0.01, 0.08))
        battery_voltage = max(20.0, battery_voltage - random.uniform(0.002, 0.01))
        vibration = max(0.0, vibration + random.uniform(-0.04, 0.04))
        oxygen_level = max(0.0, oxygen_level - random.uniform(0.001, 0.02))

        if random.random() < 0.03:
            temperature += random.uniform(8.0, 20.0)
        if random.random() < 0.02:
            vibration += random.uniform(1.2, 3.5)
        if random.random() < 0.015:
            fuel_level = max(0.0, fuel_level - random.uniform(1.5, 4.0))

        payload = {
            "timestamp": now.isoformat(),
            "spacecraft_id": spacecraft_id,
            "temperature": round(temperature, 3),
            "pressure": round(pressure, 3),
            "fuel_level": round(fuel_level, 3),
            "battery_voltage": round(battery_voltage, 3),
            "vibration": round(vibration, 3),
            "oxygen_level": round(oxygen_level, 3),
        }

        if random.random() < 0.05:
            key = random.choice(
                ["temperature", "pressure", "fuel_level", "battery_voltage", "vibration", "oxygen_level"]
            )
            payload[key] = None

        if random.random() < 0.06:
            delayed_by = random.randint(3, 15)
            payload["timestamp"] = (now - timedelta(seconds=delayed_by)).isoformat()
            payload["delayed"] = True
            payload["delay_seconds"] = delayed_by

        yield payload


def main():
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    for event in telemetry_stream(settings.spacecraft_id):
        producer.send(settings.kafka_topic_raw, value=event)
        producer.flush()
        print(f"Produced: {event}")
        time.sleep(1)


if __name__ == "__main__":
    main()
