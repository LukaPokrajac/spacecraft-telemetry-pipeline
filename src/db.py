import json
from contextlib import contextmanager
from typing import Any, Dict, Iterable, List

import psycopg2

from config import settings


@contextmanager
def get_conn():
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )
    try:
        yield conn
    finally:
        conn.close()


def insert_raw(payload: Dict[str, Any]):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO raw_telemetry(payload) VALUES (%s)", (json.dumps(payload),))
        conn.commit()


def insert_cleaned(cleaned: Dict[str, Any], notes: List[str], alerts: List[str], is_valid: bool):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cleaned_telemetry(
                    event_timestamp, spacecraft_id, temperature, pressure, fuel_level,
                    battery_voltage, vibration, oxygen_level, is_valid, validation_notes, anomaly_flags
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    cleaned["timestamp"],
                    cleaned["spacecraft_id"],
                    cleaned.get("temperature"),
                    cleaned.get("pressure"),
                    cleaned.get("fuel_level"),
                    cleaned.get("battery_voltage"),
                    cleaned.get("vibration"),
                    cleaned.get("oxygen_level"),
                    is_valid,
                    notes,
                    alerts,
                ),
            )
        conn.commit()


def insert_alerts(cleaned: Dict[str, Any], alerts: Iterable[str]):
    rows = [
        (cleaned["timestamp"], cleaned["spacecraft_id"], alert, "high" if "high" in alert else "medium", alert)
        for alert in alerts
    ]
    if not rows:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO telemetry_alerts(event_timestamp, spacecraft_id, alert_type, severity, details)
                VALUES (%s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
