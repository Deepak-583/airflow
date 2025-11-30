"""
Configuration module for ETL pipeline.
Centralizes all configuration constants and settings.
All configurations are loaded from .env file.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
# Try multiple locations: current directory, AIRFLOW_HOME, or /opt/airflow
env_paths = [
    Path(".env"),
    Path("/opt/airflow/.env"),
    Path(os.getenv("AIRFLOW_HOME", "/opt/airflow")) / ".env",
]

for env_path in env_paths:
    if env_path.exists():
        load_dotenv(env_path)
        break
else:
    # If no .env found, try loading from current directory anyway
    load_dotenv()

# Base directories
AIRFLOW_HOME = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow"))
DATA_DIR = Path(os.getenv("DATA_DIR", str(AIRFLOW_HOME / "data")))
RAW_DIR = Path(os.getenv("RAW_DIR", str(DATA_DIR / "raw")))
CLEAN_DIR = Path(os.getenv("CLEAN_DIR", str(DATA_DIR / "cleaned")))
CONFIG_DIR = Path(os.getenv("CONFIG_DIR", str(AIRFLOW_HOME / "config")))

# Ensure directories exist
RAW_DIR.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
KAFKA_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
KAFKA_SASL_MECHANISMS = os.getenv("KAFKA_SASL_MECHANISMS", "PLAIN")
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")
KAFKA_SESSION_TIMEOUT_MS = os.getenv("KAFKA_SESSION_TIMEOUT_MS", "45000")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "event")
KAFKA_POLL_TIMEOUT = int(os.getenv("KAFKA_POLL_TIMEOUT", "10"))  # seconds
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "airflow-consumer-group")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")
KAFKA_ENABLE_AUTO_COMMIT = os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "true")

# Snowflake Configuration
SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_conn")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "ANALYTICS")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "MY_SCHEMA")
SNOWFLAKE_TABLE = os.getenv("SNOWFLAKE_TABLE", "BENE")
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE", "TEMP_STAGE_FOR_AIRFLOW")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_REGION = os.getenv("SNOWFLAKE_REGION", None)
SNOWFLAKE_INSECURE_MODE = os.getenv("SNOWFLAKE_INSECURE_MODE", "true").lower() == "true"

# DAG Configuration
DEFAULT_START_DATE = os.getenv("DEFAULT_START_DATE", "2024-01-01")
DEFAULT_TAGS = os.getenv("DEFAULT_TAGS", "etl,kafka,snowflake").split(",")

