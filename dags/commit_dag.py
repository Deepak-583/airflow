"""
DAG for loading cleaned data to Snowflake BENE table.

This DAG:
1. Reads cleaned CSV files from /opt/airflow/data/cleaned
2. Maps and transforms data to match BENE table schema:
   - USER_ID, EVENT_TYPE, DESCRIPTION, ENTITY_TYPE, ENTITY_ID, SESSION_ID, PROPS, OCCURRED_AT
3. Loads data into Snowflake BENE table using COPY INTO command
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import logging
import pandas as pd
import tempfile
import json
import ast
from pathlib import Path

from config import (
    CLEAN_DIR,
    SNOWFLAKE_CONN_ID,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_TABLE,
    SNOWFLAKE_STAGE,
    DEFAULT_START_DATE,
)
from utils import (
    get_latest_file,
    ensure_directory_exists,
)

logger = logging.getLogger(__name__)


def load_to_snowflake_task(**context) -> None:
    """
    Load cleaned CSV data to Snowflake.
    
    Raises:
        FileNotFoundError: If cleaned file is not found
        Exception: If Snowflake operations fail
    """
    task_logger = logging.getLogger("airflow.task")
    task_logger.info("Starting Snowflake load process")
    
    # Get cleaned file path from multiple sources
    cleaned_file = None
    
    # Try 1: From dag_run.conf (passed by clean_dag)
    if context.get("dag_run"):
        cleaned_file = context["dag_run"].conf.get("cleaned_file")
        if cleaned_file:
            task_logger.info(f"Cleaned file from dag_run.conf: {cleaned_file}")
    
    # Try 2: From XCom (clean_dag)
    if not cleaned_file:
        ti = context["ti"]
        cleaned_file = ti.xcom_pull(
            dag_id="clean_dag",
            task_ids="clean_data"
        )
        if cleaned_file:
            task_logger.info(f"Cleaned file from XCom: {cleaned_file}")
    
    # Removed fallback to latest file to prevent reprocessing old data
    # Only process files explicitly passed from clean_dag
    
    # Validate file exists
    if not cleaned_file or not Path(cleaned_file).exists():
        raise FileNotFoundError(
            f"Cleaned file not found. This DAG should only be triggered by clean_dag. "
            f"Checked dag_run.conf and XCom. File: {cleaned_file}"
        )
    
    # Read CSV
    task_logger.info(f"Reading cleaned file: {cleaned_file}")
    df = pd.read_csv(cleaned_file)
    task_logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    task_logger.info(f"Columns in CSV: {list(df.columns)}")
    
    # Prepare DataFrame to match BENE table schema
    # Required columns in order: USER_ID, EVENT_TYPE, DESCRIPTION, ENTITY_TYPE, ENTITY_ID, SESSION_ID, PROPS, OCCURRED_AT
    
    # Create new DataFrame with required columns
    bene_df = pd.DataFrame()
    
    # Map existing columns (case-insensitive matching)
    df.columns = df.columns.str.lower()
    
    # USER_ID
    if 'user_id' in df.columns:
        bene_df['USER_ID'] = df['user_id'].astype(str)
    else:
        task_logger.warning("user_id column not found, setting to None")
        bene_df['USER_ID'] = None
    
    # EVENT_TYPE
    if 'event_type' in df.columns:
        bene_df['EVENT_TYPE'] = df['event_type'].astype(str)
    else:
        task_logger.warning("event_type column not found, setting to None")
        bene_df['EVENT_TYPE'] = None
    
    # DESCRIPTION (new field - set to None if not present)
    if 'description' in df.columns:
        bene_df['DESCRIPTION'] = df['description'].astype(str)
    else:
        task_logger.info("description column not found, setting to None")
        bene_df['DESCRIPTION'] = None
    
    # ENTITY_TYPE
    if 'entity_type' in df.columns:
        bene_df['ENTITY_TYPE'] = df['entity_type'].astype(str)
    else:
        task_logger.warning("entity_type column not found, setting to None")
        bene_df['ENTITY_TYPE'] = None
    
    # ENTITY_ID
    if 'entity_id' in df.columns:
        bene_df['ENTITY_ID'] = df['entity_id'].astype(str)
    else:
        task_logger.warning("entity_id column not found, setting to None")
        bene_df['ENTITY_ID'] = None
    
    # SESSION_ID
    if 'session_id' in df.columns:
        bene_df['SESSION_ID'] = df['session_id'].astype(str)
    else:
        task_logger.warning("session_id column not found, setting to None")
        bene_df['SESSION_ID'] = None
    
    # PROPS - Convert dict to JSON string for VARIANT type
    if 'props' in df.columns:
        def props_to_json(x):
            if pd.isna(x) or x is None:
                return '{}'
            if isinstance(x, dict):
                return json.dumps(x)
            if isinstance(x, str):
                # Already a string, try to parse and re-serialize
                try:
                    parsed = json.loads(x) if x.startswith('{') else ast.literal_eval(x)
                    return json.dumps(parsed) if isinstance(parsed, dict) else '{}'
                except:
                    return '{}'
            return '{}'
        
        bene_df['PROPS'] = df['props'].apply(props_to_json)
    else:
        task_logger.warning("props column not found, setting to empty JSON")
        bene_df['PROPS'] = '{}'
    
    # OCCURRED_AT - Map from timestamp column
    if 'timestamp' in df.columns:
        bene_df['OCCURRED_AT'] = pd.to_datetime(df['timestamp'], errors='coerce')
    elif 'occurred_at' in df.columns:
        bene_df['OCCURRED_AT'] = pd.to_datetime(df['occurred_at'], errors='coerce')
    else:
        task_logger.warning("timestamp/occurred_at column not found, setting to None")
        bene_df['OCCURRED_AT'] = None
    
    # Ensure column order matches table schema
    column_order = ['USER_ID', 'EVENT_TYPE', 'DESCRIPTION', 'ENTITY_TYPE', 'ENTITY_ID', 
                     'SESSION_ID', 'PROPS', 'OCCURRED_AT']
    bene_df = bene_df[column_order]
    
    task_logger.info(f"Prepared DataFrame for BENE table: {len(bene_df)} rows, {len(bene_df.columns)} columns")
    task_logger.info(f"Columns: {list(bene_df.columns)}")
    
    # Connect to Snowflake
    task_logger.info(f"Connecting to Snowflake using connection: {SNOWFLAKE_CONN_ID}")
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Create temporary CSV file for Snowflake with correct column order
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            bene_df.to_csv(tmp.name, index=False, header=True)
            tmp_file_path = tmp.name
        
        task_logger.info(f"Created temporary CSV: {tmp_file_path}")
        
        # Create stage if not exists
        create_stage_sql = f"CREATE STAGE IF NOT EXISTS {SNOWFLAKE_STAGE};"
        task_logger.info(f"Creating stage: {SNOWFLAKE_STAGE}")
        cursor.execute(create_stage_sql)
        
        # Upload file to Snowflake stage
        put_sql = f"PUT file://{tmp_file_path} @{SNOWFLAKE_STAGE} OVERWRITE = TRUE;"
        task_logger.info(f"Uploading file to stage: {SNOWFLAKE_STAGE}")
        cursor.execute(put_sql)
        
        # Create temporary staging table for deduplication
        staging_table = f"{SNOWFLAKE_TABLE}_STAGING_{int(datetime.now().timestamp())}"
        create_staging_sql = f"""
            CREATE TEMPORARY TABLE {staging_table} LIKE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE};
        """
        task_logger.info(f"Creating staging table: {staging_table}")
        cursor.execute(create_staging_sql)
        
        # Copy data into staging table first
        copy_sql = f"""
            COPY INTO {staging_table}
            FROM @{SNOWFLAKE_STAGE}
            FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
            ON_ERROR = 'CONTINUE';
        """
        task_logger.info(f"Copying data to staging table: {staging_table}")
        cursor.execute(copy_sql)
        
        # Merge staging data into target table with deduplication
        # Deduplicate based on USER_ID, ENTITY_ID, and OCCURRED_AT combination
        merge_sql = f"""
            MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
            USING {staging_table} AS source
            ON target.USER_ID = source.USER_ID 
               AND target.ENTITY_ID = source.ENTITY_ID 
               AND target.OCCURRED_AT = source.OCCURRED_AT
            WHEN NOT MATCHED THEN
                INSERT (USER_ID, EVENT_TYPE, DESCRIPTION, ENTITY_TYPE, ENTITY_ID, SESSION_ID, PROPS, OCCURRED_AT)
                VALUES (source.USER_ID, source.EVENT_TYPE, source.DESCRIPTION, source.ENTITY_TYPE, 
                        source.ENTITY_ID, source.SESSION_ID, source.PROPS, source.OCCURRED_AT);
        """
        task_logger.info(f"Merging data into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} (deduplicating)")
        cursor.execute(merge_sql)
        
        # Get number of rows inserted
        rows_inserted = cursor.rowcount
        task_logger.info(f"Inserted {rows_inserted} new rows (duplicates skipped)")
        
        # Commit transaction
        conn.commit()
        task_logger.info("Data successfully loaded to Snowflake")
        
        # Clean up temporary file
        Path(tmp_file_path).unlink()
        task_logger.debug(f"Cleaned up temporary file: {tmp_file_path}")
        
    except Exception as e:
        task_logger.error(f"Error loading data to Snowflake: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
        task_logger.info("Snowflake connection closed")


# DAG Definition
with DAG(
    dag_id="commit_dag",
    description="Load cleaned data to Snowflake",
    start_date=datetime.fromisoformat(DEFAULT_START_DATE),
    schedule=None,  # Triggered by clean_dag
    catchup=False,
    tags=["commit", "etl", "snowflake"],
    default_args={
        "retries": 2,
        "retry_delay": 60,
    },
) as dag:
    
    commit_task = PythonOperator(
        task_id="commit_data",
        python_callable=load_to_snowflake_task,
    )
