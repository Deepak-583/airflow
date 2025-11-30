# GCP Deployment Plan for Airflow ETL Pipeline

## Overview

This document outlines the steps to deploy the Airflow ETL pipeline to Google Cloud Platform (GCP).

## Deployment Options

### Option 1: Cloud Composer (Recommended for Managed Airflow)
- **Pros**: Fully managed, automatic scaling, integrated with GCP services
- **Cons**: Less control, higher cost, may need DAG modifications
- **Best for**: Production environments, teams wanting managed service

### Option 2: Google Kubernetes Engine (GKE)
- **Pros**: Full control, can use existing docker-compose setup, scalable
- **Cons**: More complex setup, requires Kubernetes knowledge
- **Best for**: Teams with K8s expertise, need custom configurations

### Option 3: Compute Engine (GCE) with Docker
- **Pros**: Simple migration, can use docker-compose directly
- **Cons**: Manual scaling, more maintenance
- **Best for**: Quick migration, smaller deployments

## Recommended: GKE Deployment

We'll use GKE as it provides the best balance of control and scalability.

## Pre-Deployment Checklist

### 1. GCP Services Required
- [ ] Google Kubernetes Engine (GKE) cluster
- [ ] Cloud SQL for PostgreSQL (replaces local postgres)
- [ ] Cloud Memorystore for Redis (replaces local redis)
- [ ] Cloud Storage bucket (for DAGs, logs, data)
- [ ] Artifact Registry (for Docker images)
- [ ] Secret Manager (for credentials)
- [ ] Service Account with proper permissions

### 2. Current Localhost References to Fix

#### Docker Compose Health Checks (OK - Internal)
- Health checks use `localhost` - these are fine (internal container checks)

#### Service Names (Need GCP Equivalents)
- `postgres` → Cloud SQL instance
- `redis` → Cloud Memorystore instance
- `airflow-apiserver` → Internal service name (OK)

#### Port Mappings (Need Load Balancer)
- Port `9090` → GCP Load Balancer or Ingress
- Port `8080` → Internal service (OK)

#### Database Connections
- `postgresql+psycopg2://airflow:airflow@postgres/airflow` → Cloud SQL connection
- `redis://:@redis:6379/0` → Cloud Memorystore connection

## Deployment Steps

### Phase 1: GCP Infrastructure Setup

#### Step 1: Create GCP Project and Enable APIs
```bash
# Set project
gcloud config set project YOUR_PROJECT_ID

# Enable required APIs
gcloud services enable \
  container.googleapis.com \
  sqladmin.googleapis.com \
  redis.googleapis.com \
  storage-component.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com
```

#### Step 2: Create Cloud SQL for PostgreSQL
```bash
# Create Cloud SQL instance
gcloud sql instances create airflow-postgres \
  --database-version=POSTGRES_16 \
  --tier=db-f1-micro \
  --region=us-central1 \
  --root-password=YOUR_DB_PASSWORD

# Create database
gcloud sql databases create airflow --instance=airflow-postgres

# Create user
gcloud sql users create airflow \
  --instance=airflow-postgres \
  --password=YOUR_DB_PASSWORD
```

#### Step 3: Create Cloud Memorystore for Redis
```bash
# Create Redis instance
gcloud redis instances create airflow-redis \
  --size=1 \
  --region=us-central1 \
  --redis-version=redis_7_2
```

#### Step 4: Create Cloud Storage Buckets
```bash
# Create buckets for DAGs, logs, and data
gsutil mb -p YOUR_PROJECT_ID -l us-central1 gs://YOUR_PROJECT_ID-airflow-dags
gsutil mb -p YOUR_PROJECT_ID -l us-central1 gs://YOUR_PROJECT_ID-airflow-logs
gsutil mb -p YOUR_PROJECT_ID -l us-central1 gs://YOUR_PROJECT_ID-airflow-data
```

#### Step 5: Create Artifact Registry
```bash
# Create repository for Docker images
gcloud artifacts repositories create airflow-repo \
  --repository-format=docker \
  --location=us-central1
```

#### Step 6: Create Service Account
```bash
# Create service account
gcloud iam service-accounts create airflow-sa \
  --display-name="Airflow Service Account"

# Grant necessary permissions
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:airflow-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.admin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:airflow-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/cloudsql.client"
```

#### Step 7: Store Secrets in Secret Manager
```bash
# Store database password
echo -n "YOUR_DB_PASSWORD" | gcloud secrets create airflow-db-password --data-file=-

# Store Redis host
echo -n "REDIS_IP" | gcloud secrets create airflow-redis-host --data-file=-

# Store Kafka credentials (from .env)
echo -n "KAFKA_SASL_PASSWORD" | gcloud secrets create kafka-password --data-file=-

# Store Snowflake credentials
echo -n "SNOWFLAKE_PASSWORD" | gcloud secrets create snowflake-password --data-file=-
```

### Phase 2: Build and Push Docker Image

#### Step 8: Build Docker Image
```bash
# Authenticate to Artifact Registry
gcloud auth configure-docker us-central1-docker.pkg.dev

# Build image
docker build -t us-central1-docker.pkg.dev/YOUR_PROJECT_ID/airflow-repo/airflow:latest .

# Push image
docker push us-central1-docker.pkg.dev/YOUR_PROJECT_ID/airflow-repo/airflow:latest
```

### Phase 3: Create GKE Cluster

#### Step 9: Create GKE Cluster
```bash
# Create cluster
gcloud container clusters create airflow-cluster \
  --num-nodes=3 \
  --machine-type=e2-medium \
  --region=us-central1 \
  --service-account=airflow-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com \
  --enable-cloud-logging \
  --enable-cloud-monitoring \
  --addons=HorizontalPodAutoscaling,HttpLoadBalancing

# Get credentials
gcloud container clusters get-credentials airflow-cluster --region=us-central1
```

### Phase 4: Convert Docker Compose to Kubernetes

#### Step 10: Create Kubernetes Manifests

We need to create:
- ConfigMaps for configuration
- Secrets for sensitive data
- Deployments for each service
- Services for internal communication
- Ingress for external access
- PersistentVolumeClaims for data storage

### Phase 5: Update Configuration

#### Step 11: Update Environment Variables

Create a new `.env.gcp` file with GCP-specific values:

```env
# Database (Cloud SQL)
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:PASSWORD@/airflow?host=/cloudsql/PROJECT_ID:REGION:INSTANCE_NAME
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:PASSWORD@/airflow?host=/cloudsql/PROJECT_ID:REGION:INSTANCE_NAME

# Redis (Cloud Memorystore)
AIRFLOW__CELERY__BROKER_URL=redis://REDIS_IP:6379/0

# Storage (Cloud Storage)
AIRFLOW__LOGGING__REMOTE_LOGGING=True
AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=gs://PROJECT_ID-airflow-logs
AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=google_cloud_default

# Web Server
AIRFLOW__WEBSERVER__BASE_URL=https://airflow.YOUR_DOMAIN.com
```

## Files to Create/Modify

### 1. Kubernetes Manifests
- `k8s/namespace.yaml` - Namespace
- `k8s/configmap.yaml` - Configuration
- `k8s/secrets.yaml` - Secrets (or use Secret Manager)
- `k8s/postgres-proxy.yaml` - Cloud SQL Proxy
- `k8s/airflow-scheduler.yaml` - Scheduler deployment
- `k8s/airflow-webserver.yaml` - Web server deployment
- `k8s/airflow-worker.yaml` - Worker deployment
- `k8s/airflow-dag-processor.yaml` - DAG processor
- `k8s/services.yaml` - Services
- `k8s/ingress.yaml` - Ingress for web UI

### 2. Update DAGs for Cloud Storage
- Modify file paths to use GCS
- Update data storage locations

### 3. CI/CD Pipeline
- GitHub Actions or Cloud Build
- Automated deployments

## Next Steps

1. **Choose deployment option** (recommended: GKE)
2. **Set up GCP infrastructure** (Phase 1)
3. **Build and push Docker image** (Phase 2)
4. **Create GKE cluster** (Phase 3)
5. **Convert to Kubernetes manifests** (Phase 4)
6. **Update configurations** (Phase 5)
7. **Deploy and test**

## Cost Estimation

- **GKE Cluster**: ~$150-300/month (3 nodes, e2-medium)
- **Cloud SQL**: ~$25-50/month (db-f1-micro)
- **Cloud Memorystore**: ~$30-60/month (1GB)
- **Cloud Storage**: ~$0.02/GB/month
- **Load Balancer**: ~$18/month
- **Total**: ~$223-428/month

## Security Considerations

1. Use Workload Identity for service accounts
2. Enable private GKE cluster
3. Use Cloud SQL private IP
4. Enable VPC peering for Memorystore
5. Use IAM for access control
6. Encrypt secrets in Secret Manager
7. Enable audit logging

## Monitoring and Logging

- Cloud Logging for logs
- Cloud Monitoring for metrics
- Set up alerts for DAG failures
- Monitor resource usage

