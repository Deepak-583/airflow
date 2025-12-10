"""
DAG for polling Kafka messages and saving to raw storage.

This DAG:
1. Connects to Kafka and checks for NEW messages only (not past data)
2. If new messages exist, saves them to JSON files in /opt/airflow/data/raw
3. If no new messages, skips processing and waits for next poll
4. Only triggers clean_dag if new messages were received

Behavior:
- Runs every minute
- Only reads messages that arrived since the last poll
- Commits offsets to avoid reprocessing the same data
- If no new data, returns without triggering downstream DAGs
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime
import uuid
import logging

from config import RAW_DIR, KAFKA_TOPIC, KAFKA_POLL_TIMEOUT, DEFAULT_START_DATE
from utils import consume_kafka_messages, write_json_file, ensure_directory_exists

logger = logging.getLogger(__name__)


def poll_kafka_task(**context) -> str:
    """
    Poll Kafka for NEW messages only and save to raw JSON file.
    
    This function:
    - Only reads messages that arrived since the last poll (no past data)
    - Returns None if no new messages are available
    - Commits offsets after reading to prevent reprocessing
    
    Returns:
        Path to the saved raw JSON file, or None if no new messages received
    """
    task_logger = logging.getLogger("airflow.task")
    task_logger.info(f"Starting Kafka poll for topic: {KAFKA_TOPIC}")
    task_logger.info("Checking for NEW messages only (will not read past data)")
    
    # Ensure raw directory exists
    ensure_directory_exists(RAW_DIR)
    
    # Consume NEW messages from Kafka (only messages that arrived since last poll)
    messages = consume_kafka_messages(
        topic=KAFKA_TOPIC,
        poll_timeout=KAFKA_POLL_TIMEOUT
    )
    
    if not messages:
        task_logger.info("No NEW messages received from Kafka. Skipping file creation and downstream processing.")
        task_logger.info("Will check again in next scheduled run (1 minute)")
        return None
    
    # Generate unique filename
    filename = f"{uuid.uuid4()}.json"
    filepath = str(RAW_DIR / filename)
    
    # Save messages to file
    write_json_file(filepath, messages)
    task_logger.info(f"Saved {len(messages)} NEW messages to {filepath}")
    
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
        trigger_rule="all_done",  # Will only trigger if check_messages_task returns True
        reset_dag_run=False,
    )
    
    # Only trigger clean_dag if we got NEW messages
    def check_messages(**context):
        """
        Only proceed if NEW messages were received.
        This ensures we don't trigger downstream DAGs when there's no new data.
        
        Returns:
            True if new messages were received (filepath exists), False otherwise
        """
        task_logger = logging.getLogger("airflow.task")
        ti = context['ti']
        
        # Pull XCom value from poll_kafka task
        raw_file = ti.xcom_pull(task_ids='poll_kafka', key=None)
        
        # Log the raw XCom value for debugging
        task_logger.info(f"XCom value from poll_kafka: {raw_file} (type: {type(raw_file)})")
        
        # Check if we have a valid filepath
        # Must be: not None, not empty string, not the string "None"
        if raw_file is None:
            task_logger.info("No NEW messages received from Kafka (XCom is None). Skipping downstream processing.")
            task_logger.info("This is expected behavior - will check again in next scheduled run.")
            return False
        
        # Convert to string and check
        raw_file_str = str(raw_file).strip()
        
        if not raw_file_str or raw_file_str.lower() == "none" or raw_file_str == "":
            task_logger.info(f"No NEW messages received from Kafka (XCom value: '{raw_file_str}'). Skipping downstream processing.")
            task_logger.info("This is expected behavior - will check again in next scheduled run.")
            return False
        
        # Verify the file actually exists
        from pathlib import Path
        if not Path(raw_file_str).exists():
            task_logger.warning(f"File path from XCom does not exist: {raw_file_str}. Skipping downstream processing.")
            return False
        
        # We have a valid filepath - proceed to trigger clean_dag
        task_logger.info(f"✓ NEW messages received! File: {raw_file_str}")
        task_logger.info(f"Proceeding to trigger clean_dag with file: {raw_file_str}")
        return True
    
    check_messages_task = ShortCircuitOperator(
        task_id="check_messages",
        python_callable=check_messages,
    )
    
    poll_task >> check_messages_task >> trigger_clean
