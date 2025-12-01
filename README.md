# Apache Airflow ETL Pipeline

A production-ready ETL pipeline built with Apache Airflow that processes real-time data from Kafka, cleans and transforms it, and loads it into Snowflake for analytics.

## 🚀 Overview

This project implements a complete ETL (Extract, Transform, Load) pipeline using Apache Airflow 3.1.3. The pipeline consumes event data from Kafka topics, performs data cleaning and transformation, and loads the processed data into Snowflake for business intelligence and analytics.

## 📋 Features

- **Real-time Data Processing**: Polls Kafka topics for new messages every minute
- **Data Cleaning & Transformation**: Automated data cleaning and normalization
- **Snowflake Integration**: Seamless data loading to Snowflake data warehouse
- **Docker-based Deployment**: Containerized setup for easy deployment and scaling
- **Modular Architecture**: Reusable utility modules for common operations
- **Error Handling**: Robust error handling with retry mechanisms
- **Comprehensive Logging**: Detailed logging for monitoring and debugging

## 🏗️ Architecture

The pipeline consists of three main DAGs that work together:

```
Kafka Topic → polls_dag → clean_dag → commit_dag → Snowflake
```

### Pipeline Flow

1. **polls_dag**: Polls Kafka topic for new messages and saves raw JSON files
2. **clean_dag**: Cleans and transforms raw data into structured CSV format
3. **commit_dag**: Loads cleaned data into Snowflake data warehouse

## 📁 Project Structure

```
.
├── dags/                      # Airflow DAG definitions
│   ├── polls_dag.py          # Kafka polling DAG
│   ├── clean_dag.py          # Data cleaning DAG
│   ├── commit_dag.py         # Snowflake loading DAG
│   ├── config.py             # Centralized configuration
│   └── utils/                # Utility modules
│       ├── file_utils.py     # File operations
│       ├── kafka_utils.py    # Kafka consumer functions
│       └── data_utils.py     # Data cleaning functions
├── scripts/                   # Helper scripts
│   ├── kafka/                # Kafka producer/consumer scripts
│   └── setup_snowflake_connection.py
├── config/                    # Configuration files
│   ├── airflow.cfg           # Airflow configuration
│   └── kafka/                # Kafka configuration
├── data/                      # Data directories
│   ├── raw/                  # Raw JSON files from Kafka
│   └── cleaned/              # Cleaned CSV files
├── docker-compose.yaml        # Docker Compose configuration
├── Dockerfile                # Custom Airflow image
├── requirements.txt          # Python dependencies
└── GCP_DEPLOYMENT_PLAN.md   # GCP deployment guide
```

## 🛠️ Technology Stack

- **Apache Airflow 3.1.3**: Workflow orchestration
- **Apache Kafka**: Real-time event streaming
- **Snowflake**: Cloud data warehouse
- **Docker & Docker Compose**: Containerization
- **Python 3.12**: Programming language
- **PostgreSQL**: Airflow metadata database
- **Redis**: Celery message broker

## 📦 Dependencies

Key Python packages:
- `apache-airflow-providers-apache-kafka`: Kafka integration
- `apache-airflow-providers-snowflake`: Snowflake integration
- `kafka-python`: Kafka client library
- `confluent-kafka`: Confluent Kafka client
- `snowflake-connector-python`: Snowflake connector
- `pandas`: Data manipulation
- `python-dotenv`: Environment variable management

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose installed
- Kafka cluster accessible (or local Kafka setup)
- Snowflake account with appropriate permissions
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/Deepak-583/airflow.git
   cd airflow
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Configure Kafka**
   - Place your Kafka `client.properties` file in `config/kafka/`
   - Ensure Kafka topic is accessible

4. **Configure Snowflake**
   - Set up Snowflake connection in Airflow UI (Admin → Connections)
   - Connection ID: `snowflake_conn` (or configure in `config.py`)

5. **Start the services**
   ```bash
   docker-compose up -d
   ```

6. **Access Airflow UI**
   - Open http://localhost:8080
   - Default credentials: `airflow` / `airflow`

## ⚙️ Configuration

### Environment Variables

Key environment variables (set in `.env` file):

- `AIRFLOW_PROJ_DIR`: Base path for Airflow files (default: `/home/deepakbablu667/airflow`)
- `AIRFLOW_UID`: User ID for Airflow containers (default: `50000`)
- `KAFKA_TOPIC`: Kafka topic name (default: `event`)
- `KAFKA_POLL_TIMEOUT`: Poll timeout in seconds (default: `10`)
- `SNOWFLAKE_CONN_ID`: Airflow connection ID (default: `snowflake_conn`)
- `SNOWFLAKE_DATABASE`: Snowflake database (default: `ANALYTICS`)
- `SNOWFLAKE_SCHEMA`: Snowflake schema (default: `MY_SCHEMA`)
- `SNOWFLAKE_TABLE`: Snowflake table (default: `TEST`)

### DAG Configuration

All DAGs are configured in `dags/config.py` with support for environment variable overrides.

## 📊 DAGs Overview

### polls_dag
- **Schedule**: Every minute (`*/1 * * * *`)
- **Purpose**: Polls Kafka topic for new messages
- **Output**: Raw JSON files in `data/raw/`
- **Triggers**: `clean_dag` on successful completion

### clean_dag
- **Schedule**: Triggered by `polls_dag`
- **Purpose**: Cleans and transforms raw event data
- **Input**: Raw JSON files from `polls_dag`
- **Output**: Cleaned CSV files in `data/cleaned/`
- **Triggers**: `commit_dag` on successful completion

### commit_dag
- **Schedule**: Triggered by `clean_dag`
- **Purpose**: Loads cleaned data to Snowflake
- **Input**: Cleaned CSV files from `clean_dag`
- **Output**: Data in Snowflake table

## 🔧 Development

### Running Locally

1. Ensure all services are running:
   ```bash
   docker-compose ps
   ```

2. Check DAGs in Airflow UI

3. Trigger DAGs manually or wait for scheduled runs

### Testing

Each DAG can be tested independently:
- **polls_dag**: Test Kafka connection and message consumption
- **clean_dag**: Test data cleaning with sample JSON files
- **commit_dag**: Test Snowflake connection and data loading

### Utility Scripts

- `scripts/kafka/producer.py`: Produce test messages to Kafka
- `scripts/kafka/consumer.py`: Consume messages from Kafka
- `scripts/setup_snowflake_connection.py`: Setup Snowflake connection

## ☁️ Deployment

### GCP Deployment

See `GCP_DEPLOYMENT_PLAN.md` for detailed deployment instructions. Options include:

- **Cloud Composer**: Fully managed Airflow (recommended for production)
- **Google Kubernetes Engine (GKE)**: Full control with Kubernetes
- **Compute Engine (GCE)**: Simple VM-based deployment

### Production Considerations

- Use Cloud SQL for PostgreSQL instead of local postgres
- Use Cloud Memorystore for Redis instead of local redis
- Store DAGs in Cloud Storage
- Use Secret Manager for credentials
- Set up proper monitoring and alerting
- Configure backup strategies

## 📝 Best Practices

1. **Separation of Concerns**: Business logic separated from DAG definitions
2. **Code Reusability**: Common functions in utility modules
3. **Configuration Management**: Centralized config with environment variable support
4. **Error Handling**: Proper exception handling and logging
5. **Documentation**: Docstrings and comments throughout
6. **Type Hints**: Type annotations for better code clarity
7. **Logging**: Structured logging at appropriate levels
8. **Retries**: Default retry configuration for all DAGs

## 🐛 Troubleshooting

### Common Issues

1. **DAGs not appearing**: Check DAG folder permissions and Airflow logs
2. **Kafka connection errors**: Verify Kafka configuration and network connectivity
3. **Snowflake connection errors**: Check connection credentials in Airflow UI
4. **Permission errors**: Ensure proper file permissions for data directories

### Logs

View logs for specific services:
```bash
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
docker-compose logs airflow-worker
```

## 📚 Documentation

- [DAGs README](dags/README.md): Detailed DAG documentation
- [GCP Deployment Plan](GCP_DEPLOYMENT_PLAN.md): GCP deployment guide
- [Kafka Scripts README](scripts/kafka/README.md): Kafka utilities documentation

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.

## 👤 Author

**Deepak Kalukuri**
- GitHub: [@Deepak-583](https://github.com/Deepak-583)

## 🙏 Acknowledgments

- Apache Airflow community
- Apache Kafka community
- Snowflake documentation

---

**Note**: This configuration is optimized for development. For production deployments, refer to the GCP deployment plan and follow security best practices.

