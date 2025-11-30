"""
DAG for polling Kafka messages and saving to raw storage.

This DAG:
1. Connects to Kafka and consumes messages
2. Saves raw messages to JSON files in /opt/airflow/data/raw
3. Triggers clean_dag to process the data
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import uuid
import logging

from config import RAW_DIR, KAFKA_TOPIC, KAFKA_POLL_TIMEOUT, DEFAULT_START_DATE
from utils import consume_kafka_messages, write_json_file, ensure_directory_exists

logger = logging.getLogger(__name__)


def poll_kafka_task(**context) -> str:
    """
    Poll Kafka for messages and save to raw JSON file.
    
    Returns:
        Path to the saved raw JSON file, or None if no messages received
    """
    task_logger = logging.getLogger("airflow.task")
    task_logger.info(f"Starting Kafka poll for topic: {KAFKA_TOPIC}")
    
    # Ensure raw directory exists
    ensure_directory_exists(RAW_DIR)
    
    # Consume messages from Kafka
    messages = consume_kafka_messages(
        topic=KAFKA_TOPIC,
        poll_timeout=KAFKA_POLL_TIMEOUT
    )
    
    if not messages:
        task_logger.warning("No messages received from Kafka. Skipping file creation.")
        return None
    
    # Generate unique filename
    filename = f"{uuid.uuid4()}.json"
    filepath = str(RAW_DIR / filename)
    
    # Save messages to file
    write_json_file(filepath, messages)
    task_logger.info(f"Saved {len(messages)} messages to {filepath}")
    
    return filepath




# DAG Definition
with DAG(
    dag_id="polls_dag",
    description="Poll Kafka messages and save to raw storage",
    start_date=datetime.fromisoformat(DEFAULT_START_DATE),
    schedule="*/1 * * * *",  # Run every minute
    catchup=False,
    tags=["kafka", "etl", "poll"],
    default_args={
        "retries": 2,
        "retry_delay": 60,  # 1 minute
    },
) as dag:
    
    poll_task = PythonOperator(
        task_id="poll_kafka",
        python_callable=poll_kafka_task,
    )
    
    trigger_clean = TriggerDagRunOperator(
        task_id="trigger_clean_dag",
        trigger_dag_id="clean_dag",
        conf={"raw_file": "{{ ti.xcom_pull(task_ids='poll_kafka') }}"},
        wait_for_completion=False,
        trigger_rule="all_success",  # Trigger if poll_kafka succeeded
        reset_dag_run=False,
    )
    
    poll_task >> trigger_clean
