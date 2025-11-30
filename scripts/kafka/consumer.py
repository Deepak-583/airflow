"""
Kafka Consumer Script

Receives events from a Kafka topic and optionally saves them to a file.
Can be used standalone or as a utility script.
"""
import json
import sys
import os
from pathlib import Path
from confluent_kafka import Consumer
from confluent_kafka import KafkaError, KafkaException
from dotenv import load_dotenv

# Load environment variables from .env file
# Try multiple locations: current directory, project root, or /opt/airflow
PROJECT_ROOT = Path(__file__).parent.parent.parent
env_paths = [
    Path(".env"),
    PROJECT_ROOT / ".env",
    Path("/opt/airflow/.env"),
]

for env_path in env_paths:
    if env_path.exists():
        load_dotenv(env_path)
        break
else:
    # If no .env found, try loading from current directory anyway
    load_dotenv()


def read_config():
    """Reads the client configuration from environment variables"""
    config = {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""),
        "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanisms": os.getenv("KAFKA_SASL_MECHANISMS", "PLAIN"),
        "sasl.username": os.getenv("KAFKA_SASL_USERNAME", ""),
        "sasl.password": os.getenv("KAFKA_SASL_PASSWORD", ""),
        "session.timeout.ms": os.getenv("KAFKA_SESSION_TIMEOUT_MS", "45000"),
        "client.id": os.getenv("KAFKA_CLIENT_ID", ""),
    }
    
    # Remove empty values
    config = {k: v for k, v in config.items() if v}
    
    if "bootstrap.servers" not in config or not config["bootstrap.servers"]:
        raise ValueError(
            "Kafka configuration incomplete. Missing KAFKA_BOOTSTRAP_SERVERS. "
            "Please ensure .env file exists and contains valid Kafka configuration."
        )
    
    return config


def consume_events(topic, config, max_messages=None, output_file=None):
    """
    Consumes events from Kafka topic and displays them
    
    Args:
        topic: Kafka topic name
        config: Kafka configuration
        max_messages: Maximum number of messages to consume (None for unlimited)
        output_file: Optional file path to save consumed events as JSON
    """
    # Set consumer group and offset
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"
    
    # Create consumer
    consumer = Consumer(config)
    
    # Subscribe to topic
    consumer.subscribe([topic])
    
    print(f"👂 Listening to topic '{topic}'...")
    if max_messages:
        print(f"   Max messages: {max_messages}")
    if output_file:
        print(f"   Output file: {output_file}")
    print("   Press Ctrl+C to stop\n")
    
    message_count = 0
    consumed_events = []
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                if message_count == 0:
                    print("⏳ Waiting for messages...")
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            # Deserialize message
            try:
                event = json.loads(msg.value().decode('utf-8'))
                message_count += 1
                key = msg.key().decode('utf-8') if msg.key() else None
                
                # Store event
                event_data = {
                    **event,
                    "_kafka_metadata": {
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "key": key
                    }
                }
                consumed_events.append(event_data)
                
                # Display event
                print(f"\n{'='*60}")
                print(f"📨 Message #{message_count}")
                print(f"{'='*60}")
                print(f"Key: {key}")
                print(f"Partition: {msg.partition()}, Offset: {msg.offset()}")
                print(f"Event Type: {event.get('event_type')}")
                print(f"User ID: {event.get('user_id')}")
                print(f"Entity: {event.get('entity_type')} - {event.get('entity_id')}")
                print(f"Timestamp: {event.get('timestamp')}")
                
                # Stop after max_messages if specified
                if max_messages and message_count >= max_messages:
                    print(f"\n✅ Received {message_count} messages (limit reached)")
                    break
                    
            except json.JSONDecodeError as e:
                print(f"⚠️ Failed to decode message at offset {msg.offset()}: {e}")
            except Exception as e:
                print(f"⚠️ Error processing message: {e}")
                
    except KeyboardInterrupt:
        print(f"\n\n⏹️  Consumer interrupted. Received {message_count} messages total.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        # Save to file if specified
        if output_file and consumed_events:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(consumed_events, f, indent=2)
                print(f"\n💾 Saved {len(consumed_events)} events to {output_file}")
            except Exception as e:
                print(f"\n❌ Error saving to file: {e}")
        
        consumer.close()
        print("🔒 Consumer closed")
        
        # Print summary
        if message_count > 0:
            print(f"\n{'='*60}")
            print("📊 CONSUMPTION SUMMARY")
            print(f"{'='*60}")
            print(f"✅ Total messages consumed: {message_count}")
            print(f"📡 Topic: {topic}")
            print(f"{'='*60}")


def main():
    """Main function to consume events"""
    # Default configuration
    default_topic = "event"
    default_max_messages = None  # None means unlimited
    default_output_file = None
    
    # Parse command line arguments
    topic = default_topic
    max_messages = default_max_messages
    output_file = default_output_file
    
    # Simple argument parser
    # Usage: python consumer.py [topic] [max_messages] [output_file]
    if len(sys.argv) > 1:
        topic = sys.argv[1]
    if len(sys.argv) > 2:
        try:
            max_messages = int(sys.argv[2])
        except ValueError:
            print(f"⚠️  Invalid max_messages value, using unlimited")
    if len(sys.argv) > 3:
        output_file = sys.argv[3]
    
    print("="*60)
    print("KAFKA CONSUMER - Receiving Events from Kafka")
    print("="*60)
    print(f"📡 Topic: {topic}")
    print(f"🔢 Max messages: {max_messages if max_messages else 'Unlimited'}")
    print(f"💾 Output file: {output_file if output_file else 'Console only'}")
    print(f"⚙️  Config: Environment variables (.env)")
    print("="*60)
    
    try:
        config = read_config()
        consume_events(topic, config, max_messages, output_file)
    except KeyboardInterrupt:
        print("\n\n⏹️  Consumer stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

