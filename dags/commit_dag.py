"""
DAG for loading cleaned data to Snowflake BENE table.

This DAG:
1. Reads cleaned CSV files from /opt/airflow/data/cleaned
2. Maps and transforms data to match BENE table schema:
   - USER_ID, EVENT_TYPE, DESCRIPTION, ENTITY_TYPE, ENTITY_ID, SESSION_ID, PROPS, OCCURRED_AT, USERNAME (9 columns)
   - Note: USERNAME is in the last position to match Snowflake table schema
3. Loads data into Snowflake BENE table using COPY INTO command
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta, timezone
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
    MAX_EVENT_AGE_DAYS,
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
    
    # Validate CSV is not empty
    if len(df) == 0:
        task_logger.warning("⚠️ Cleaned CSV file is empty. No data to process.")
        task_logger.info("Skipping Snowflake load - no data to insert.")
        return  # Exit early, no need to process empty data
    
    # Prepare DataFrame to match BENE table schema
    # Required columns in order: USER_ID, EVENT_TYPE, DESCRIPTION, ENTITY_TYPE, ENTITY_ID, SESSION_ID, PROPS, OCCURRED_AT, USERNAME (9 columns)
    # Note: USERNAME is in the last position to match Snowflake table schema
    
    # Create new DataFrame with required columns
    # Initialize with same index as source DataFrame to maintain row alignment
    bene_df = pd.DataFrame(index=df.index)
    
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
    
    # PROPS - Convert to JSON string for VARIANT type
    # Note: props should already be JSON string from clean_dag CSV
    if 'props' in df.columns:
        def props_to_json(x):
            """Convert props to valid JSON string for Snowflake VARIANT type"""
            if pd.isna(x) or x is None:
                return '{}'
            if isinstance(x, str):
                # Should already be JSON string from clean_dag
                x_str = str(x).strip()
                if not x_str or x_str == '' or x_str.lower() == 'nan':
                    return '{}'
                # Validate it's valid JSON
                try:
                    parsed = json.loads(x_str)
                    # Re-serialize to ensure it's properly formatted
                    return json.dumps(parsed)
                except json.JSONDecodeError:
                    # Try parsing as Python literal as fallback
                    try:
                        parsed = ast.literal_eval(x_str)
                        if isinstance(parsed, (dict, list)):
                            return json.dumps(parsed)
                        else:
                            task_logger.warning(f"Props parsed to non-dict/list type: {type(parsed)}")
                            return '{}'
                    except (ValueError, SyntaxError) as e:
                        task_logger.warning(f"Failed to parse props: {x_str[:100]}, error: {e}")
                        return '{}'
            if isinstance(x, dict) or isinstance(x, list):
                # If somehow still a dict/list, convert to JSON
                try:
                    return json.dumps(x)
                except (TypeError, ValueError) as e:
                    task_logger.warning(f"Failed to serialize props dict/list: {e}")
                    return '{}'
            return '{}'
        
        bene_df['PROPS'] = df['props'].apply(props_to_json)
        
        # Log sample of props data for debugging
        sample_props = bene_df['PROPS'].head(3).tolist()
        task_logger.info(f"Sample PROPS values (first 3): {sample_props}")
        non_empty_props = bene_df[bene_df['PROPS'] != '{}']['PROPS'].count()
        task_logger.info(f"PROPS column: {non_empty_props} non-empty values out of {len(bene_df)} total rows")
    else:
        task_logger.warning("props column not found, setting to empty JSON")
        bene_df['PROPS'] = '{}'
    
    # OCCURRED_AT - Map from timestamp column
    # IMPORTANT: Preserve original timezone if present, otherwise assume UTC
    # This prevents incorrect timezone conversions that change the actual event time
    if 'timestamp' in df.columns:
        # First, try to parse with timezone awareness (preserves original timezone)
        # If timestamp has timezone info (like 'Z' or '+05:30'), it will be preserved
        # If it's naive (no timezone), we'll assume it's already in UTC
        timestamps = pd.to_datetime(df['timestamp'], errors='coerce')
        # If timestamps are naive (no timezone), assume they're UTC
        if timestamps.dt.tz is None:
            timestamps = timestamps.dt.tz_localize('UTC')
        else:
            # Convert to UTC but preserve the original time value
            # This ensures the actual event time is preserved, not shifted
            timestamps = timestamps.dt.tz_convert('UTC')
        bene_df['OCCURRED_AT'] = timestamps
        task_logger.info(f"Parsed timestamps - sample: {timestamps.head(3).tolist()}")
    elif 'occurred_at' in df.columns:
        timestamps = pd.to_datetime(df['occurred_at'], errors='coerce')
        if timestamps.dt.tz is None:
            timestamps = timestamps.dt.tz_localize('UTC')
        else:
            timestamps = timestamps.dt.tz_convert('UTC')
        bene_df['OCCURRED_AT'] = timestamps
        task_logger.info(f"Parsed timestamps - sample: {timestamps.head(3).tolist()}")
    else:
        task_logger.warning("timestamp/occurred_at column not found, setting to None")
        bene_df['OCCURRED_AT'] = None
    
    # USERNAME - String datatype (last position to match Snowflake table schema)
    if 'username' in df.columns:
        bene_df['USERNAME'] = df['username'].astype(str)
    else:
        task_logger.info("username column not found, setting to None")
        bene_df['USERNAME'] = None
    
    # Ensure column order matches Snowflake table schema (9 columns)
    # Order: USER_ID, EVENT_TYPE, DESCRIPTION, ENTITY_TYPE, ENTITY_ID, SESSION_ID, PROPS, OCCURRED_AT, USERNAME
    column_order = ['USER_ID', 'EVENT_TYPE', 'DESCRIPTION', 'ENTITY_TYPE', 'ENTITY_ID', 
                     'SESSION_ID', 'PROPS', 'OCCURRED_AT', 'USERNAME']
    bene_df = bene_df[column_order]
    
    # Deduplicate within the batch to ensure idempotency
    # Keep only the latest version of each event (based on USER_ID, ENTITY_ID, OCCURRED_AT)
    # This prevents old deleted data from being re-inserted when the same event appears again
    initial_row_count = len(bene_df)
    
    # Drop duplicates based on the unique key (USER_ID, ENTITY_ID, OCCURRED_AT)
    # Keep the last occurrence to ensure we have the most recent data
    # This ensures that if the same event appears multiple times in the batch, only the latest is kept
    bene_df = bene_df.drop_duplicates(
        subset=['USER_ID', 'ENTITY_ID', 'OCCURRED_AT'],
        keep='last'
    )
    
    deduplicated_count = len(bene_df)
    if initial_row_count != deduplicated_count:
        task_logger.info(f"Deduplicated batch: {initial_row_count} rows -> {deduplicated_count} rows "
                        f"(removed {initial_row_count - deduplicated_count} duplicates)")
    else:
        task_logger.info(f"No duplicates found in batch: {deduplicated_count} unique rows")
    
    # Filter out old/historical events to prevent re-inserting deleted data
    # Only process events that are recent (within MAX_EVENT_AGE_DAYS)
    # This prevents the website from re-inserting old historical data when new activity happens
    # Example: On Dec 17, events from Dec 10 (7 days old) will be filtered out if MAX_EVENT_AGE_DAYS=1
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=MAX_EVENT_AGE_DAYS)
    before_filter_count = len(bene_df)
    
    # Filter out events older than cutoff_date
    if 'OCCURRED_AT' in bene_df.columns:
        # Convert OCCURRED_AT to datetime if it's not already
        # Use utc=True to ensure timezone-aware datetimes for proper comparison
        bene_df['OCCURRED_AT'] = pd.to_datetime(bene_df['OCCURRED_AT'], errors='coerce', utc=True)
        
        # Ensure cutoff_date is timezone-aware (UTC) to match OCCURRED_AT
        # This prevents "Invalid comparison between dtype=datetime64[ns, UTC] and datetime" error
        if cutoff_date.tzinfo is None:
            cutoff_date = cutoff_date.replace(tzinfo=timezone.utc)
        
        # Log sample of event dates for debugging
        if len(bene_df) > 0:
            sample_dates = bene_df['OCCURRED_AT'].head(5).tolist()
            oldest_event = bene_df['OCCURRED_AT'].min()
            newest_event = bene_df['OCCURRED_AT'].max()
            task_logger.info(f"Event date range: {oldest_event} to {newest_event}")
            task_logger.info(f"Cutoff date (events older than this will be filtered): {cutoff_date}")
        
        # Filter to only recent events (events must be >= cutoff_date)
        # Also filter out NaT (Not a Time) values that result from failed datetime parsing
        bene_df = bene_df[(bene_df['OCCURRED_AT'] >= cutoff_date) & (bene_df['OCCURRED_AT'].notna())]
        
        filtered_count = len(bene_df)
        old_events_filtered = before_filter_count - filtered_count
        
        if old_events_filtered > 0:
            task_logger.warning(f"⚠️ FILTERED OUT {old_events_filtered} OLD/HISTORICAL EVENTS "
                              f"(older than {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')})")
            task_logger.warning(f"This prevents re-inserting deleted historical data. "
                              f"Only processing {filtered_count} recent events (within last {MAX_EVENT_AGE_DAYS} day(s)).")
            current_date = datetime.now(timezone.utc) if cutoff_date.tzinfo else datetime.now()
            task_logger.warning(f"Current date: {current_date}, Cutoff: {cutoff_date}")
        else:
            task_logger.info(f"All {filtered_count} events are recent (within last {MAX_EVENT_AGE_DAYS} day(s))")
    else:
        task_logger.warning("OCCURRED_AT column not found, cannot filter old events. "
                          "All events will be processed (this may cause old data to be re-inserted).")
    
    # Check if DataFrame is empty after filtering
    if len(bene_df) == 0:
        task_logger.warning("⚠️ No events to process after filtering. All events were either too old or invalid.")
        task_logger.info("Skipping Snowflake load - no data to insert.")
        return  # Exit early, no need to connect to Snowflake
    
    task_logger.info(f"Prepared DataFrame for BENE table: {len(bene_df)} rows, {len(bene_df.columns)} columns")
    task_logger.info(f"Columns: {list(bene_df.columns)}")
    
    # Sort by OCCURRED_AT to ensure chronological order (newest first for easier debugging)
    # This helps with debugging and ensures consistent ordering
    if 'OCCURRED_AT' in bene_df.columns:
        bene_df = bene_df.sort_values('OCCURRED_AT', ascending=False)
        task_logger.info(f"Sorted DataFrame by OCCURRED_AT (newest first)")
        task_logger.info(f"Date range after sorting: {bene_df['OCCURRED_AT'].min()} to {bene_df['OCCURRED_AT'].max()}")
    
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
        
        # Clean up any existing files in the stage from previous runs
        # This prevents leftover historical data from being re-inserted
        cleanup_stage_sql = f"REMOVE @{SNOWFLAKE_STAGE};"
        task_logger.info(f"Cleaning up stage: {SNOWFLAKE_STAGE} (removing any leftover files)")
        try:
            cursor.execute(cleanup_stage_sql)
            task_logger.info("Stage cleaned successfully")
        except Exception as e:
            # Stage might be empty, which is fine
            task_logger.debug(f"Stage cleanup note: {e}")
        
        # Upload file to Snowflake stage
        put_sql = f"PUT file://{tmp_file_path} @{SNOWFLAKE_STAGE} OVERWRITE = TRUE;"
        task_logger.info(f"Uploading file to stage: {SNOWFLAKE_STAGE}")
        cursor.execute(put_sql)
        
        # Create temporary staging table for deduplication
        # Use timezone-aware datetime for consistent timestamp generation
        staging_table = f"{SNOWFLAKE_TABLE}_STAGING_{int(datetime.now(timezone.utc).timestamp())}"
        create_staging_sql = f"""
            CREATE TEMPORARY TABLE {staging_table} LIKE {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE};
        """
        task_logger.info(f"Creating staging table: {staging_table}")
        cursor.execute(create_staging_sql)
        
        # Copy data into staging table first
        # Note: COPY INTO maps by position, so CSV column order must match table column order
        # We ensure the CSV has columns in the correct order before uploading
        copy_sql = f"""
            COPY INTO {staging_table}
            FROM @{SNOWFLAKE_STAGE}
            FILE_FORMAT = (TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"')
            ON_ERROR = 'CONTINUE';
        """
        task_logger.info(f"Copying data to staging table: {staging_table}")
        task_logger.info(f"CSV column order: {list(bene_df.columns)}")
        task_logger.info("Note: COPY INTO maps by position, so CSV order must match table schema order")
        cursor.execute(copy_sql)
        
        # Merge staging data into target table with idempotency
        # CRITICAL FIX: Only insert records that are recent (within MAX_EVENT_AGE_DAYS)
        # This prevents re-inserting old deleted records that are in the staging layer
        # The WHERE clause in WHEN NOT MATCHED ensures only truly new, recent events are inserted
        # Calculate cutoff timestamp - use DATEADD to work with Snowflake's timestamp types
        cutoff_timestamp = (datetime.now(timezone.utc) - timedelta(days=MAX_EVENT_AGE_DAYS)).strftime('%Y-%m-%d %H:%M:%S')
        
        merge_sql = f"""
            MERGE INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} AS target
            USING {staging_table} AS source
            ON target.USER_ID = source.USER_ID 
               AND target.ENTITY_ID = source.ENTITY_ID 
               AND target.OCCURRED_AT = source.OCCURRED_AT
            WHEN MATCHED THEN
                UPDATE SET
                    EVENT_TYPE = source.EVENT_TYPE,
                    DESCRIPTION = source.DESCRIPTION,
                    ENTITY_TYPE = source.ENTITY_TYPE,
                    SESSION_ID = source.SESSION_ID,
                    PROPS = source.PROPS,
                    USERNAME = source.USERNAME
            WHEN NOT MATCHED AND source.OCCURRED_AT >= DATEADD(day, -{MAX_EVENT_AGE_DAYS}, CURRENT_TIMESTAMP()) THEN
                INSERT (USER_ID, EVENT_TYPE, DESCRIPTION, ENTITY_TYPE, ENTITY_ID, SESSION_ID, PROPS, OCCURRED_AT, USERNAME)
                VALUES (source.USER_ID, source.EVENT_TYPE, source.DESCRIPTION, source.ENTITY_TYPE, 
                        source.ENTITY_ID, source.SESSION_ID, source.PROPS, source.OCCURRED_AT, source.USERNAME);
        """
        task_logger.info(f"Merging data into {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} "
                        f"(idempotent: updates existing, inserts only recent events)")
        task_logger.info(f"⚠️ CRITICAL: Only inserting events with OCCURRED_AT >= {cutoff_timestamp} "
                        f"(within last {MAX_EVENT_AGE_DAYS} day(s)) to prevent re-inserting deleted old data")
        
        # Get count of rows in staging before merge for logging
        count_sql = f"SELECT COUNT(*) FROM {staging_table};"
        cursor.execute(count_sql)
        staging_count = cursor.fetchone()[0]
        task_logger.info(f"Staging table contains {staging_count} rows to merge")
        
        # Count how many rows in staging are recent enough to be inserted
        recent_count_sql = f"SELECT COUNT(*) FROM {staging_table} WHERE OCCURRED_AT >= DATEADD(day, -{MAX_EVENT_AGE_DAYS}, CURRENT_TIMESTAMP());"
        cursor.execute(recent_count_sql)
        recent_count = cursor.fetchone()[0]
        old_count = staging_count - recent_count
        if old_count > 0:
            task_logger.warning(f"⚠️ {old_count} rows in staging are too old (older than {cutoff_timestamp}) "
                              f"and will NOT be inserted. This prevents re-inserting deleted historical data.")
        task_logger.info(f"Only {recent_count} recent rows will be considered for insertion")
        
        # Execute merge
        cursor.execute(merge_sql)
        
        # Get number of rows affected (inserted + updated)
        # Note: Snowflake's rowcount returns the total number of rows affected by the MERGE
        rows_affected = cursor.rowcount
        task_logger.info(f"Merge completed: {rows_affected} rows affected "
                        f"(combination of new inserts and existing row updates)")
        task_logger.info("✅ Idempotency ensured: Old deleted records will NOT be re-inserted due to date filter in MERGE")
        
        # Clean up staging table (temporary tables are auto-dropped, but explicit cleanup is good practice)
        # Also clean up the stage to remove any leftover files
        try:
            cleanup_stage_sql = f"REMOVE @{SNOWFLAKE_STAGE};"
            cursor.execute(cleanup_stage_sql)
            task_logger.info("Cleaned up stage after merge (removed uploaded files)")
        except Exception as e:
            task_logger.debug(f"Stage cleanup note: {e}")
        
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
