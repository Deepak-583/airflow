# Setup Steps After Pulling from Git

Follow these steps after pulling the repository from git to get your Airflow pipeline running.

## Prerequisites

- Docker and Docker Compose installed
- Git repository cloned/pulled
- Access to your Snowflake account credentials
- Access to your Kafka credentials (if using Kafka)

## Step-by-Step Setup

### 1. Navigate to the Project Directory

```bash
cd airflow_repo  # or whatever your project folder is named
```

### 2. Create and Configure `.env` File

The `.env` file is **required** and contains all your configuration. Create it from scratch or copy from an example:

```bash
# Create .env file (if it doesn't exist)
touch .env
```

**Add the following environment variables to `.env`:**

```env
# Airflow Configuration
AIRFLOW_PROJ_DIR=/opt/airflow
AIRFLOW_UID=50000

# Snowflake Configuration (REQUIRED - Update with your credentials)
SNOWFLAKE_CONN_ID=snowflake_conn
SNOWFLAKE_ACCOUNT=YOUR_ACCOUNT_NAME
SNOWFLAKE_HOST=YOUR_ACCOUNT.snowflakecomputing.com
SNOWFLAKE_WAREHOUSE=YOUR_WAREHOUSE
SNOWFLAKE_DATABASE=YOUR_DATABASE
SNOWFLAKE_SCHEMA=YOUR_SCHEMA
SNOWFLAKE_TABLE=BENE
SNOWFLAKE_STAGE=TEMP_STAGE_FOR_AIRFLOW
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_USER=YOUR_USERNAME
SNOWFLAKE_PASSWORD=YOUR_PASSWORD
SNOWFLAKE_REGION=  # Optional, leave empty if not needed
SNOWFLAKE_INSECURE_MODE=true

# Kafka Configuration (if using Kafka)
KAFKA_BOOTSTRAP_SERVERS=your-kafka-servers
KAFKA_TOPIC=event
KAFKA_POLL_TIMEOUT=10
KAFKA_CONSUMER_GROUP=airflow-consumer-group
# Add other Kafka credentials as needed

# DAG Configuration
DEFAULT_START_DATE=2024-01-01
```

**Important:** Replace all `YOUR_*` placeholders with your actual Snowflake credentials.

### 3. Start Docker Services

```bash
# Start all services in detached mode
docker compose up -d

# Wait for services to initialize (about 1-2 minutes)
# Check status
docker compose ps
```

### 4. Wait for Services to be Healthy

Wait for all services to show as "healthy":

```bash
# Check service health
docker compose ps

# Watch logs to ensure services are starting properly
docker compose logs -f airflow-scheduler
# Press Ctrl+C to stop watching
```

**Expected healthy services:**
- `airflow-scheduler` - should be "healthy"
- `airflow-dag-processor` - should be "healthy"
- `postgres` - should be "healthy"
- `redis` - should be "healthy"

### 5. Set Up Snowflake Connection in Airflow

After services are running, configure the Snowflake connection:

```bash
# Run the setup script to create/update Snowflake connection
docker compose exec airflow-scheduler python /opt/airflow/scripts/setup_snowflake_connection.py
```

**Expected output:**
```
✅ Successfully configured connection: snowflake_conn
   Account: YOUR_ACCOUNT
   Warehouse: YOUR_WAREHOUSE
   Database: YOUR_DATABASE
   Role: ACCOUNTADMIN
```

### 6. Verify DAGs are Loaded

Check that all DAGs are recognized:

```bash
# List all DAGs
docker compose exec airflow-scheduler airflow dags list

# Check for import errors
docker compose exec airflow-scheduler airflow dags list-import-errors
```

**Expected output:** You should see:
- `clean_dag`
- `commit_dag`
- `polls_dag`

And **no import errors**.

### 7. Access Airflow UI (Optional)

If the webserver is running (check for port conflicts):

```bash
# Check if webserver is running
docker compose ps | grep webserver

# Access UI at:
# http://localhost:8080 or http://localhost:9090
# Default credentials: airflow / airflow
```

**Note:** If port 8080 is in use, the webserver may not start, but core services (scheduler, worker) will still work.

### 8. Unpause DAGs (When Ready)

DAGs are paused by default. To enable them:

**Option A: Via Airflow UI**
1. Go to Airflow UI → DAGs
2. Toggle the switch next to each DAG to unpause

**Option B: Via CLI**
```bash
# Unpause all DAGs
docker compose exec airflow-scheduler airflow dags unpause polls_dag
docker compose exec airflow-scheduler airflow dags unpause clean_dag
docker compose exec airflow-scheduler airflow dags unpause commit_dag
```

## Verification Checklist

✅ `.env` file created with all required variables  
✅ Docker services started and healthy  
✅ Snowflake connection configured  
✅ All 3 DAGs loaded without errors  
✅ No import errors in DAGs  
✅ Services are running (scheduler, worker, dag-processor)

## Troubleshooting

### Port Already in Use
If you see "port is already allocated" errors:
```bash
# Check what's using the port
# Windows: netstat -ano | findstr :8080
# Linux/Mac: lsof -i :8080

# The core services (scheduler, worker) will still work even if webserver can't start
```

### DAG Import Errors
If DAGs fail to import:
```bash
# Check logs
docker compose logs airflow-dag-processor

# Verify config.py exists
docker compose exec airflow-scheduler ls -la /opt/airflow/dags/
```

### Snowflake Connection Issues
```bash
# Verify connection exists
docker compose exec airflow-scheduler airflow connections get snowflake_conn

# Re-run setup script
docker compose exec airflow-scheduler python /opt/airflow/scripts/setup_snowflake_connection.py
```

## Next Steps

Once everything is set up:
1. **Test the pipeline**: Unpause `polls_dag` to start processing
2. **Monitor logs**: Watch DAG execution in Airflow UI or via logs
3. **Check Snowflake**: Verify data is being loaded to your Snowflake table

## Quick Start Commands Summary

```bash
# 1. Create .env file and add your credentials
# 2. Start services
docker compose up -d

# 3. Wait for services (1-2 minutes), then setup Snowflake
docker compose exec airflow-scheduler python /opt/airflow/scripts/setup_snowflake_connection.py

# 4. Verify DAGs
docker compose exec airflow-scheduler airflow dags list

# 5. Unpause DAGs when ready
docker compose exec airflow-scheduler airflow dags unpause polls_dag
```

---

**Important:** Never commit your `.env` file to git! It contains sensitive credentials.

