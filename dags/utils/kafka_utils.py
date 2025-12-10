"""
Kafka utility functions for consuming messages.
"""
import logging
from datetime import datetime
from typing import List, Optional
from confluent_kafka import Consumer
from confluent_kafka.error import KafkaError

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_MECHANISMS,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    KAFKA_SESSION_TIMEOUT_MS,
    KAFKA_CLIENT_ID,
    KAFKA_TOPIC,
    KAFKA_POLL_TIMEOUT,
    KAFKA_CONSUMER_GROUP,
    KAFKA_AUTO_OFFSET_RESET,
    KAFKA_ENABLE_AUTO_COMMIT,
)

logger = logging.getLogger(__name__)


def read_kafka_config() -> dict:
    """
    Read Kafka configuration from environment variables.
    
    Returns:
        Dictionary with Kafka consumer settings
        
    Raises:
        ValueError: If bootstrap.servers is not found in config
    """
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
        "sasl.mechanisms": KAFKA_SASL_MECHANISMS,
        "sasl.username": KAFKA_SASL_USERNAME,
        "sasl.password": KAFKA_SASL_PASSWORD,
        "session.timeout.ms": KAFKA_SESSION_TIMEOUT_MS,
        "client.id": KAFKA_CLIENT_ID,
        "group.id": KAFKA_CONSUMER_GROUP,
        "auto.offset.reset": KAFKA_AUTO_OFFSET_RESET,
        "enable.auto.commit": KAFKA_ENABLE_AUTO_COMMIT,
    }
    
    # Remove empty values
    config = {k: v for k, v in config.items() if v}
    
    # Validate required configuration
    if "bootstrap.servers" not in config or not config["bootstrap.servers"]:
        raise ValueError(
            "Kafka configuration incomplete. Missing KAFKA_BOOTSTRAP_SERVERS. "
            "Please ensure .env file exists and contains valid Kafka configuration."
        )
    
    logger.info("Kafka config loaded from environment variables")
    logger.debug(f"Bootstrap servers: {config.get('bootstrap.servers', 'N/A')}")
    
    return config


def consume_kafka_messages(
    topic: str = KAFKA_TOPIC,
    poll_timeout: int = KAFKA_POLL_TIMEOUT,
    max_messages: Optional[int] = None
) -> List[str]:
    """
    Consume NEW messages from Kafka topic (only messages that arrived since last poll).
    This function ensures that:
    1. Only new messages are read (not past data)
    2. Offsets are properly committed to avoid reprocessing
    3. If no new messages, returns empty list
    
    Args:
        topic: Kafka topic name
        poll_timeout: Maximum time to poll for messages (seconds)
        max_messages: Maximum number of messages to consume (None for unlimited)
        
    Returns:
        List of message values as strings (only new messages)
    """
    logger.info(f"Connecting to Kafka topic: {topic} to check for NEW messages only")
    
    # Read Kafka configuration
    kafka_config = read_kafka_config()
    
    # Ensure auto-commit is disabled for manual control
    if kafka_config.get("enable.auto.commit", "false").lower() == "true":
        logger.warning("Auto-commit is enabled. Disabling for manual offset control.")
        kafka_config["enable.auto.commit"] = "false"
    
    # Ensure we only read new messages (latest offset)
    if kafka_config.get("auto.offset.reset", "latest").lower() != "latest":
        logger.warning("auto.offset.reset is not 'latest'. Setting to 'latest' to only read new messages.")
        kafka_config["auto.offset.reset"] = "latest"
    
    # Create consumer
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])
    
    messages = []
    start_time = datetime.now()
    
    logger.info(f"Polling Kafka for NEW messages (up to {poll_timeout} seconds)...")
    logger.info("Note: Only messages that arrived after consumer start will be read (no past data)")
    
    try:
        # Wait for partition assignment (required for offset management)
        assignment_wait_time = 5  # seconds
        assignment_start = datetime.now()
        while (datetime.now() - assignment_start).total_seconds() < assignment_wait_time:
            partitions = consumer.assignment()
            if partitions:
                logger.info(f"Assigned to partitions: {[p.partition for p in partitions]}")
                break
            consumer.poll(timeout=0.1)
        
        # Poll for new messages
        while (datetime.now() - start_time).total_seconds() < poll_timeout:
            # Check if we've reached max messages
            if max_messages and len(messages) >= max_messages:
                logger.info(f"Reached max messages limit: {max_messages}")
                break
            
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # No message available, continue polling until timeout
                continue
            
            # Handle errors
            if msg.error():
                error_code = msg.error().code()
                if error_code == KafkaError._PARTITION_EOF:
                    # End of partition reached - no more messages available
                    logger.debug("Reached end of partition (no more messages)")
                    break
                else:
                    logger.warning(f"Kafka error: {msg.error()}")
                    continue
            
            # Decode message (this is a NEW message)
            try:
                message_value = msg.value().decode("utf-8")
                messages.append(message_value)
                logger.debug(f"Received NEW message #{len(messages)} from partition {msg.partition()}, offset {msg.offset()}")
            except Exception as e:
                logger.warning(f"Error decoding message: {e}")
                continue
        
        # Commit offsets ONLY after successfully consuming messages
        # This ensures we don't reprocess the same messages
        if messages:
            try:
                consumer.commit(asynchronous=False)
                logger.info(f"Successfully committed offsets for {len(messages)} new messages")
            except Exception as e:
                logger.error(f"Failed to commit offsets: {e}. Messages may be reprocessed!")
                raise
        else:
            logger.info("No new messages received from Kafka")
    
    finally:
        consumer.close()
        logger.info(f"Total NEW messages received: {len(messages)}")
    
    return messages

