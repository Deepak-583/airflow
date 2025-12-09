# Fix Snowflake Connection 404 Error

## Problem

The error `404 Not Found: post HO88822.snowflakecomputing.com:443/session/v1/login-request` indicates the Snowflake host is incorrect.

**Issue:** The host is missing the account locator/region identifier.

## Solution

### Step 1: Check Current Connection Configuration

```bash
# Check current Snowflake connection
docker compose exec airflow-scheduler airflow connections get snowflake_conn
```

### Step 2: Verify Your .env File

Check your `.env` file and ensure `SNOWFLAKE_HOST` has the correct format:

**Correct format:**
```env
SNOWFLAKE_HOST=RJTQTNM-HO88822.snowflakecomputing.com
```

**Incorrect format (what's causing the error):**
```env
SNOWFLAKE_HOST=HO88822.snowflakecomputing.com
```

### Step 3: Update .env File

Edit your `.env` file and ensure:

```env
SNOWFLAKE_ACCOUNT=HO88822
SNOWFLAKE_HOST=RJTQTNM-HO88822.snowflakecomputing.com
SNOWFLAKE_USER=GNCIPL
SNOWFLAKE_PASSWORD=YourPassword
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=MY_SCHEMA
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

**Important:** 
- `SNOWFLAKE_ACCOUNT` = Just the account name (e.g., `HO88822`)
- `SNOWFLAKE_HOST` = Full hostname with account locator (e.g., `RJTQTNM-HO88822.snowflakecomputing.com`)

### Step 4: Re-run Setup Script

After updating `.env`, re-run the setup script to update the connection:

```bash
# Re-run setup script to update connection
docker compose exec airflow-scheduler python /opt/airflow/scripts/setup_snowflake_connection.py
```

### Step 5: Verify Connection

```bash
# Check the connection was updated
docker compose exec airflow-scheduler airflow connections get snowflake_conn
```

You should see the correct host: `RJTQTNM-HO88822.snowflakecomputing.com`

### Step 6: Test the Connection

```bash
# Try to test the connection (if enabled)
docker compose exec airflow-scheduler airflow connections test snowflake_conn
```

### Step 7: Restart Services (if needed)

```bash
# Restart scheduler to pick up connection changes
docker compose restart airflow-scheduler airflow-worker
```

## Alternative: Update Connection via Airflow UI

If you prefer to update via UI:

1. Go to Airflow UI → Admin → Connections
2. Find `snowflake_conn`
3. Update the **Host** field to: `RJTQTNM-HO88822.snowflakecomputing.com`
4. Save the connection

## How to Find Your Correct Snowflake Host

Your Snowflake host format depends on your account type:

1. **Standard Account Format:**
   - Host: `{account_locator}-{account_name}.snowflakecomputing.com`
   - Example: `RJTQTNM-HO88822.snowflakecomputing.com`

2. **VPS Account Format:**
   - Host: `{account_name}.{region}.snowflakecomputing.com`
   - Example: `HO88822.us-east-1.snowflakecomputing.com`

3. **Find it in Snowflake:**
   - Log into Snowflake web UI
   - Check the URL: `https://{your-host}/...`
   - Or check your Snowflake account details

## Quick Fix Commands

```bash
# 1. Update .env file with correct SNOWFLAKE_HOST
# Edit .env and set: SNOWFLAKE_HOST=RJTQTNM-HO88822.snowflakecomputing.com

# 2. Re-run setup script
docker compose exec airflow-scheduler python /opt/airflow/scripts/setup_snowflake_connection.py

# 3. Verify
docker compose exec airflow-scheduler airflow connections get snowflake_conn | grep host

# 4. Restart if needed
docker compose restart airflow-scheduler
```

## Expected Result

After fixing, the connection should work and you should see:
- Connection host: `RJTQTNM-HO88822.snowflakecomputing.com`
- No more 404 errors
- Successful data loading to Snowflake

