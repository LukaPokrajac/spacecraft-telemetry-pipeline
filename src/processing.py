from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple


SAFE_RANGES = {
    "temperature": (-40.0, 85.0),
    "pressure": (90.0, 115.0),
    "fuel_level": (0.0, 100.0),
    "battery_voltage": (20.0, 32.0),
    "vibration": (0.0, 5.0),
    "oxygen_level": (0.0, 100.0),
}


def _to_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def clean_and_validate(record: Dict[str, Any], previous: Dict[str, Any] | None) -> Tuple[Dict[str, Any], List[str], bool]:
    notes: List[str] = []
    cleaned = dict(record)

    if "timestamp" not in record or "spacecraft_id" not in record:
        return cleaned, ["missing_required_fields"], False

    try:
        event_dt = _to_dt(str(record["timestamp"]))
        cleaned["timestamp"] = event_dt.isoformat()
    except Exception:
        notes.append("invalid_timestamp")
        cleaned["timestamp"] = datetime.now(timezone.utc).isoformat()

    numeric_fields = ["temperature", "pressure", "fuel_level", "battery_voltage", "vibration", "oxygen_level"]
    for field in numeric_fields:
        value = record.get(field)
        if value is None:
            if previous and previous.get(field) is not None:
                cleaned[field] = previous[field]
                notes.append(f"missing_{field}_filled_from_previous")
            else:
                cleaned[field] = None
                notes.append(f"missing_{field}")
            continue

        try:
            f_value = float(value)
            min_v, max_v = SAFE_RANGES[field]
            if f_value < min_v or f_value > max_v:
                notes.append(f"{field}_out_of_range")
            cleaned[field] = f_value
        except Exception:
            cleaned[field] = None
            notes.append(f"{field}_non_numeric")

    is_valid = not any(n in notes for n in ["missing_required_fields", "invalid_timestamp"])
    return cleaned, notes, is_valid


def detect_anomalies(current: Dict[str, Any], previous: Dict[str, Any] | None, last_seen_at: datetime | None) -> List[str]:
    alerts: List[str] = []

    temperature = current.get("temperature")
    if temperature is not None and temperature > 65:
        alerts.append("temperature_high")

    vibration = current.get("vibration")
    if vibration is not None and vibration > 3.0:
        alerts.append("vibration_spike")

    fuel = current.get("fuel_level")
    prev_fuel = previous.get("fuel_level") if previous else None
    if fuel is not None and prev_fuel is not None and (prev_fuel - fuel) > 1.0:
        alerts.append("fuel_drop_fast")

    battery = current.get("battery_voltage")
    prev_battery = previous.get("battery_voltage") if previous else None
    if battery is not None and prev_battery is not None and (prev_battery - battery) > 0.2:
        alerts.append("battery_drop_fast")

    if last_seen_at is not None:
        current_ts = _to_dt(current["timestamp"])
        if (current_ts - last_seen_at).total_seconds() > 6:
            alerts.append("signal_gap_detected")

    return alerts
