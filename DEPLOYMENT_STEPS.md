# Step-by-Step GCP Deployment Guide

## Quick Start: What We Need to Do

### Current Issues with Localhost
1. ✅ **Health checks** - These are fine (internal container checks)
2. ❌ **Database connections** - Need Cloud SQL connection strings
3. ❌ **Redis connection** - Need Cloud Memorystore connection
4. ❌ **Port mappings** - Need Load Balancer/Ingress
5. ❌ **File storage** - Need Cloud Storage buckets
6. ❌ **Docker images** - Need Artifact Registry

## Immediate Next Steps

### Step 1: Prepare GCP Environment
```bash
# 1. Set your GCP project
export PROJECT_ID="your-project-id"
export REGION="us-central1"
gcloud config set project $PROJECT_ID

# 2. Enable required APIs
gcloud services enable container.googleapis.com
gcloud services enable sqladmin.googleapis.com
gcloud services enable redis.googleapis.com
gcloud services enable storage-component.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable secretmanager.googleapis.com
```

### Step 2: Get Connection Information
```bash
# Get Cloud SQL connection name (we'll create this)
# Format: PROJECT_ID:REGION:INSTANCE_NAME

# Get Redis IP (we'll create this)
# Format: IP_ADDRESS:6379
```

### Step 3: Create GCP-Specific Configuration
We need to:
1. Create `.env.gcp` with GCP connection strings
2. Update docker-compose or create Kubernetes manifests
3. Build and push Docker image to Artifact Registry
4. Deploy to GKE

## What Needs to Change

### Configuration Changes Required

#### 1. Database Connection
**Current:**
```env
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
```

**GCP (Cloud SQL):**
```env
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:PASSWORD@/airflow?host=/cloudsql/PROJECT_ID:REGION:INSTANCE_NAME
```

#### 2. Redis Connection
**Current:**
```env
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
```

**GCP (Cloud Memorystore):**
```env
AIRFLOW__CELERY__BROKER_URL=redis://REDIS_IP:6379/0
```

#### 3. Storage Paths
**Current:**
- Local file system: `/opt/airflow/data`

**GCP:**
- Cloud Storage: `gs://PROJECT_ID-airflow-data/`

#### 4. Web Server URL
**Current:**
- Local: `http://localhost:9090`

**GCP:**
- Load Balancer: `https://airflow.your-domain.com`

## Action Items

### Immediate (Before Deployment)
- [ ] Create GCP project and enable APIs
- [ ] Create Cloud SQL instance
- [ ] Create Cloud Memorystore instance
- [ ] Create Cloud Storage buckets
- [ ] Create Artifact Registry
- [ ] Create service account with permissions
- [ ] Store secrets in Secret Manager

### Short Term (Deployment)
- [ ] Build and push Docker image
- [ ] Create GKE cluster
- [ ] Convert docker-compose to Kubernetes manifests
- [ ] Create ConfigMaps and Secrets
- [ ] Deploy services to GKE
- [ ] Set up Ingress/Load Balancer
- [ ] Test deployment

### Long Term (Optimization)
- [ ] Set up CI/CD pipeline
- [ ] Configure monitoring and alerts
- [ ] Optimize costs
- [ ] Set up backup strategies
- [ ] Configure auto-scaling

## Questions to Answer

1. **Which GCP region?** (e.g., us-central1, asia-south1)
2. **What's your GCP project ID?**
3. **Do you have a domain for the web UI?**
4. **What's your budget for infrastructure?**
5. **Do you prefer managed (Cloud Composer) or self-managed (GKE)?**

## Next Action

**Choose one:**
1. **Start with GKE setup** - I'll create Kubernetes manifests
2. **Start with Cloud Composer** - I'll prepare for managed Airflow
3. **Create infrastructure scripts** - I'll create Terraform/scripts
4. **Update configuration files** - I'll modify .env and configs for GCP

Let me know which approach you prefer!

