# ETL Pipeline DAGs

This directory contains the Airflow DAGs for the Kafka → Clean → Snowflake ETL pipeline.

## Project Structure

```
dags/
├── config.py              # Centralized configuration
├── polls_dag.py           # Polls Kafka and saves raw data
├── clean_dag.py           # Cleans and transforms data
├── commit_dag.py          # Loads data to Snowflake
├── utils/                 # Shared utility modules
│   ├── __init__.py
│   ├── file_utils.py      # File operations
│   ├── kafka_utils.py     # Kafka consumer functions
│   └── data_utils.py      # Data cleaning functions
└── README.md              # This file
```

## DAGs Overview

### 1. polls_dag
- **Schedule**: Every minute (`*/1 * * * *`)
- **Purpose**: Polls Kafka topic for new messages
- **Output**: Raw JSON files in `/opt/airflow/data/raw`
- **Triggers**: `clean_dag`

### 2. clean_dag
- **Schedule**: Triggered by `polls_dag`
- **Purpose**: Cleans and transforms raw event data
- **Input**: Raw JSON files from `polls_dag`
- **Output**: Cleaned CSV files in `/opt/airflow/data/cleaned`
- **Triggers**: `commit_dag`

### 3. commit_dag
- **Schedule**: Triggered by `clean_dag`
- **Purpose**: Loads cleaned data to Snowflake
- **Input**: Cleaned CSV files from `clean_dag`
- **Output**: Data in Snowflake table

## Configuration

All configuration is centralized in `config.py`. Key settings:

- **Kafka**: Topic, poll timeout, consumer group
- **Snowflake**: Connection ID, database, schema, table
- **Paths**: Data directories

Environment variables can override defaults:
- `KAFKA_TOPIC` - Kafka topic name (default: "event")
- `KAFKA_POLL_TIMEOUT` - Poll timeout in seconds (default: 10)
- `SNOWFLAKE_CONN_ID` - Airflow connection ID (default: "snowflake_conn")
- `SNOWFLAKE_DATABASE` - Snowflake database (default: "ANALYTICS")
- `SNOWFLAKE_SCHEMA` - Snowflake schema (default: "MY_SCHEMA")
- `SNOWFLAKE_TABLE` - Snowflake table (default: "TEST")

## Utilities

### file_utils.py
- `get_latest_file()` - Get most recent file in directory
- `read_json_file()` - Read and parse JSON files
- `write_json_file()` - Write data to JSON files
- `ensure_directory_exists()` - Create directories if needed

### kafka_utils.py
- `read_kafka_config()` - Read Kafka config from client.properties
- `consume_kafka_messages()` - Consume messages from Kafka topic

### data_utils.py
- `clean_event_data()` - Clean and normalize event data
- `normalize_dataframe_columns()` - Normalize column names

## Best Practices Applied

1. **Separation of Concerns**: Business logic separated from DAG definitions
2. **Code Reusability**: Common functions in utility modules
3. **Configuration Management**: Centralized config with environment variable support
4. **Error Handling**: Proper exception handling and logging
5. **Documentation**: Docstrings and comments throughout
6. **Type Hints**: Type annotations for better code clarity
7. **Logging**: Structured logging at appropriate levels
8. **Retries**: Default retry configuration for all DAGs

## Setup

1. Ensure `client.properties` exists in `/opt/airflow/config/`
2. Configure Snowflake connection in Airflow UI (Admin → Connections)
3. Set environment variables if needed
4. DAGs will be automatically discovered by Airflow

## Testing

Each DAG can be tested independently:
- `polls_dag`: Test Kafka connection and message consumption
- `clean_dag`: Test data cleaning with sample JSON files
- `commit_dag`: Test Snowflake connection and data loading

