import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic_raw: str = os.getenv("KAFKA_TOPIC_RAW", "spacecraft.telemetry.raw")
    kafka_consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "telemetry-processor-group")
    postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    postgres_db: str = os.getenv("POSTGRES_DB", "telemetry")
    postgres_user: str = os.getenv("POSTGRES_USER", "telemetry_user")
    postgres_password: str = os.getenv("POSTGRES_PASSWORD", "telemetry_pass")
    spacecraft_id: str = os.getenv("SPACECRAFT_ID", "SC-001")


settings = Settings()
