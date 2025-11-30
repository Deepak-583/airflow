#!/usr/bin/env python3
"""
Script to set up Snowflake connection in Airflow.
This script reads the connection configuration from environment variables and creates/updates the connection.
"""
import json
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add parent directory to path to import Airflow modules
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load environment variables from .env file
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

try:
    from airflow.models import Connection
    from airflow.settings import Session
except ImportError:
    print("Error: This script must be run inside Airflow environment")
    print("Run: docker compose exec airflow-scheduler python /opt/airflow/scripts/setup_snowflake_connection.py")
    sys.exit(1)


def load_connection_config():
    """Load Snowflake connection configuration from environment variables."""
    config = {
        "conn_id": os.getenv("SNOWFLAKE_CONN_ID", "snowflake_conn"),
        "conn_type": "snowflake",
        "description": "Snowflake connection for ETL pipeline",
        "host": os.getenv("SNOWFLAKE_HOST", ""),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", ""),
        "login": os.getenv("SNOWFLAKE_USER", ""),
        "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
        "port": None,
        "extra": {
            "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
            "database": os.getenv("SNOWFLAKE_DATABASE", ""),
            "region": os.getenv("SNOWFLAKE_REGION") or None,
            "role": os.getenv("SNOWFLAKE_ROLE", ""),
            "private_key_file": None,
            "private_key_content": None,
            "insecure_mode": os.getenv("SNOWFLAKE_INSECURE_MODE", "true").lower() == "true",
        }
    }
    
    # Validate required fields
    required_fields = ["host", "account", "warehouse", "database"]
    missing_fields = [field for field in required_fields if not config.get(field) and not config.get("extra", {}).get(field)]
    
    if missing_fields:
        print(f"Error: Missing required environment variables: {', '.join(missing_fields)}")
        print("Please ensure .env file exists and contains all required Snowflake configuration.")
        sys.exit(1)
    
    return config


def setup_connection():
    """Create or update Snowflake connection in Airflow."""
    config = load_connection_config()
    
    session = Session()
    
    # Check if connection already exists
    existing_conn = session.query(Connection).filter(
        Connection.conn_id == config["conn_id"]
    ).first()
    
    if existing_conn:
        print(f"Updating existing connection: {config['conn_id']}")
        # Update existing connection
        existing_conn.conn_type = config["conn_type"]
        existing_conn.host = config["host"]
        existing_conn.schema = config.get("schema")
        existing_conn.login = config.get("login", "")
        existing_conn.password = config.get("password", "")
        existing_conn.port = config.get("port")
        existing_conn.extra = json.dumps(config.get("extra", {}))
        existing_conn.description = config.get("description", "")
    else:
        print(f"Creating new connection: {config['conn_id']}")
        # Create new connection
        new_conn = Connection(
            conn_id=config["conn_id"],
            conn_type=config["conn_type"],
            host=config["host"],
            schema=config.get("schema"),
            login=config.get("login", ""),
            password=config.get("password", ""),
            port=config.get("port"),
            extra=json.dumps(config.get("extra", {})),
            description=config.get("description", "")
        )
        session.add(new_conn)
    
    session.commit()
    session.close()
    
    print(f"✅ Successfully configured connection: {config['conn_id']}")
    print(f"   Account: {config['extra'].get('account')}")
    print(f"   Warehouse: {config['extra'].get('warehouse')}")
    print(f"   Database: {config['extra'].get('database')}")
    print(f"   Role: {config['extra'].get('role')}")
    print("\n⚠️  Note: You still need to set your username and password in Airflow UI:")
    print(f"   1. Go to Admin → Connections → {config['conn_id']}")
    print("   2. Update 'Login' and 'Password' fields")
    print("   3. Test the connection")


if __name__ == "__main__":
    setup_connection()

