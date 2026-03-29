import pandas as pd
import psycopg2
import streamlit as st

from config import settings


st.set_page_config(page_title="Spacecraft Telemetry Dashboard", layout="wide")
st.title("Spacecraft Telemetry Dashboard")


@st.cache_data(ttl=5)
def query_df(sql: str) -> pd.DataFrame:
    conn = psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )
    try:
        return pd.read_sql(sql, conn)
    finally:
        conn.close()


latest = query_df(
    """
    SELECT event_timestamp, spacecraft_id, temperature, pressure, fuel_level, battery_voltage, vibration, oxygen_level
    FROM cleaned_telemetry
    ORDER BY event_timestamp DESC
    LIMIT 1
    """
)

if latest.empty:
    st.warning("No telemetry yet. Start producer and consumer.")
else:
    row = latest.iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Temperature", f"{row['temperature']:.2f}")
    c2.metric("Fuel", f"{row['fuel_level']:.2f}%")
    c3.metric("Battery", f"{row['battery_voltage']:.2f}V")
    c4.metric("Vibration", f"{row['vibration']:.2f}")


st.subheader("Recent Alerts")
alerts = query_df(
    """
    SELECT event_timestamp, spacecraft_id, alert_type, severity, details
    FROM telemetry_alerts
    ORDER BY event_timestamp DESC
    LIMIT 20
    """
)
st.dataframe(alerts, use_container_width=True)


st.subheader("Sensor Trends")
trend = query_df(
    """
    SELECT event_timestamp, temperature, fuel_level, battery_voltage, vibration
    FROM cleaned_telemetry
    ORDER BY event_timestamp DESC
    LIMIT 300
    """
)
if not trend.empty:
    trend = trend.sort_values("event_timestamp")
    st.line_chart(
        trend.set_index("event_timestamp")[["temperature", "fuel_level", "battery_voltage", "vibration"]]
    )
