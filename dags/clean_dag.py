"""
DAG for cleaning and transforming raw data.

This DAG:
1. Reads raw JSON files from /opt/airflow/data/raw
2. Cleans and transforms the data
3. Saves cleaned data as CSV to /opt/airflow/data/cleaned
4. Triggers commit_dag to load data to Snowflake
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# Removed unused import

from datetime import datetime
import uuid
import logging
import pandas as pd

from config import RAW_DIR, CLEAN_DIR, DEFAULT_START_DATE
from utils import (
    read_json_file,
    get_latest_file,
    clean_event_data,
    normalize_dataframe_columns,
    ensure_directory_exists,
)

logger = logging.getLogger(__name__)


def clean_data_task(**context) -> str:
    """
    Clean raw data and save as CSV.
    
    Returns:
        Path to the cleaned CSV file
    """
    task_logger = logging.getLogger("airflow.task")
    task_logger.info("Starting data cleaning process")
    
    # Ensure cleaned directory exists
    ensure_directory_exists(CLEAN_DIR)
    
    # Get raw file path from multiple sources
    raw_file = None
    
    # Try 1: From dag_run.conf (passed by polls_dag)
    if context.get("dag_run"):
        raw_file = context["dag_run"].conf.get("raw_file")
        if raw_file and raw_file != "None" and raw_file.strip():
            task_logger.info(f"Raw file from dag_run.conf: {raw_file}")
        else:
            raw_file = None
    
    # Try 2: From XCom (polls_dag)
    if not raw_file:
        ti = context["ti"]
        raw_file = ti.xcom_pull(
            dag_id="polls_dag",
            task_ids="poll_kafka"
        )
        if raw_file and raw_file != "None" and raw_file.strip():
            task_logger.info(f"Raw file from XCom: {raw_file}")
        else:
            raw_file = None
    
    # Try 3: Latest file in raw directory (fallback)
    if not raw_file:
        raw_file = get_latest_file(RAW_DIR, ".json")
        if raw_file:
            task_logger.info(f"Using latest raw file: {raw_file}")
    
    # Validate file exists and is not None
    if not raw_file or raw_file == "None":
        error_msg = (
            f"No raw file found. This usually means polls_dag received no messages from Kafka. "
            f"Checked dag_run.conf, XCom, and {RAW_DIR}. "
            f"Please ensure Kafka has messages and polls_dag is running successfully."
        )
        task_logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # Validate file path is valid
    from pathlib import Path
    if not Path(raw_file).exists():
        error_msg = f"Raw file path does not exist: {raw_file}"
        task_logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # Read raw data
    task_logger.info(f"Reading raw file: {raw_file}")
    raw_data = read_json_file(raw_file)
    
    # Clean data
    task_logger.info(f"Cleaning {len(raw_data)} records")
    cleaned_records = clean_event_data(raw_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(cleaned_records)
    df = normalize_dataframe_columns(df)
    
    task_logger.info(f"DataFrame shape: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # Save cleaned CSV
    filename = f"{uuid.uuid4()}.csv"
    cleaned_filepath = str(CLEAN_DIR / filename)
    df.to_csv(cleaned_filepath, index=False)
    
    task_logger.info(f"Cleaned file saved: {cleaned_filepath}")
    
    return cleaned_filepath


# DAG Definition
with DAG(
    dag_id="clean_dag",
    description="Clean and transform raw event data",
    start_date=datetime.fromisoformat(DEFAULT_START_DATE),
    schedule=None,  # Triggered by polls_dag
    catchup=False,
    tags=["clean", "etl", "transform"],
    default_args={
        "retries": 2,
        "retry_delay": 60,
    },
) as dag:
    
    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data_task,
    )
    
    trigger_commit = TriggerDagRunOperator(
        task_id="trigger_commit_dag",
        trigger_dag_id="commit_dag",
        conf={"cleaned_file": "{{ ti.xcom_pull(task_ids='clean_data') }}"},
        wait_for_completion=False,
    )
    
    clean_task >> trigger_commit
