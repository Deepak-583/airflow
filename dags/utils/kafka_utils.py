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
    Consume messages from Kafka topic.
    
    Args:
        topic: Kafka topic name
        poll_timeout: Maximum time to poll for messages (seconds)
        max_messages: Maximum number of messages to consume (None for unlimited)
        
    Returns:
        List of message values as strings
    """
    logger.info(f"Connecting to Kafka topic: {topic}")
    
    # Read Kafka configuration
    kafka_config = read_kafka_config()
    
    # Create consumer
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])
    
    messages = []
    start_time = datetime.now()
    
    logger.info(f"Polling Kafka for up to {poll_timeout} seconds...")
    
    try:
        while (datetime.now() - start_time).total_seconds() < poll_timeout:
            # Check if we've reached max messages
            if max_messages and len(messages) >= max_messages:
                logger.info(f"Reached max messages limit: {max_messages}")
                break
            
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            # Handle errors
            if msg.error():
                error_code = msg.error().code()
                if error_code == KafkaError._PARTITION_EOF:
                    # End of partition, continue polling
                    continue
                else:
                    logger.warning(f"Kafka error: {msg.error()}")
                    continue
            
            # Decode message
            try:
                message_value = msg.value().decode("utf-8")
                messages.append(message_value)
                logger.debug(f"Received message #{len(messages)}")
            except Exception as e:
                logger.warning(f"Error decoding message: {e}")
                continue
    
    finally:
        consumer.close()
        logger.info(f"Total messages received: {len(messages)}")
    
    return messages

