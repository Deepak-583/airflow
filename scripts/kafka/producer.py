"""
Kafka Producer Script

Sends events from a JSON file to a Kafka topic.
Can be used standalone or as a utility script.
"""
import json
import sys
import os
from pathlib import Path
from confluent_kafka import Producer
from confluent_kafka import KafkaError
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


def produce_events_from_json(topic, config, json_file_path):
    """
    Produces events from JSON file to Kafka topic
    
    Args:
        topic: Kafka topic name
        config: Kafka configuration
        json_file_path: Path to JSON file with events
    """
    # Create producer
    producer = Producer(config)
    
    # Load events from JSON file
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            events = json.load(f)
        print(f"✅ Loaded {len(events)} events from {json_file_path}")
    except FileNotFoundError:
        print(f"❌ Error: File not found at {json_file_path}")
        print("   Please check the file path and try again.")
        return False
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON file - {e}")
        return False
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return False
    
    # Track delivery status
    delivered_count = 0
    failed_count = 0
    
    def delivery_callback(err, msg):
        """Callback for message delivery confirmation"""
        nonlocal delivered_count, failed_count
        if err:
            print(f"❌ Message delivery failed: {err}")
            failed_count += 1
        else:
            delivered_count += 1
            # Print progress every 50 messages
            if delivered_count % 50 == 0:
                print(f"   ✅ Sent {delivered_count}/{len(events)} events...")
    
    print(f"\n📤 Sending {len(events)} events to topic '{topic}'...")
    print("   This may take a moment...\n")
    
    # Produce each event
    for event in events:
        # Use user_id as key for partitioning (ensures same user goes to same partition)
        key = event.get('user_id', '').encode('utf-8') if event.get('user_id') else None
        
        # Serialize event to JSON
        value = json.dumps(event).encode('utf-8')
        
        # Produce message
        producer.produce(
            topic,
            key=key,
            value=value,
            callback=delivery_callback
        )
        
        # Poll to trigger delivery callbacks
        producer.poll(0)
    
    # Wait for all messages to be delivered (with timeout)
    print("\n⏳ Waiting for all messages to be delivered...")
    remaining = producer.flush(timeout=60)
    
    # Print final results
    print(f"\n{'='*60}")
    print("📊 PRODUCTION SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Successfully delivered: {delivered_count}")
    if failed_count > 0:
        print(f"❌ Failed: {failed_count}")
    if remaining > 0:
        print(f"⚠️  Not delivered (timeout): {remaining}")
    print(f"📦 Total events in file: {len(events)}")
    print(f"📡 Topic: {topic}")
    print(f"{'='*60}")
    
    if delivered_count == len(events):
        print(f"\n✅ All events successfully sent to Kafka!")
        return True
    else:
        print(f"\n⚠️  Some events may not have been delivered.")
        return False


def main():
    """Main function to produce events"""
    # Default configuration
    default_topic = "event"
    default_json_file = "data.json"
    
    # Get file path from command line argument or use default
    if len(sys.argv) > 1:
        json_file_path = sys.argv[1]
    else:
        json_file_path = default_json_file
    
    # Get topic from command line argument or use default
    if len(sys.argv) > 2:
        topic = sys.argv[2]
    else:
        topic = default_topic
    
    print("="*60)
    print("KAFKA PRODUCER - Sending Events to Kafka")
    print("="*60)
    print(f"📁 File: {json_file_path}")
    print(f"📡 Topic: {topic}")
    print(f"⚙️  Config: Environment variables (.env)")
    print("="*60)
    
    try:
        config = read_config()
        success = produce_events_from_json(topic, config, json_file_path)
        
        if not success:
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Production interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

