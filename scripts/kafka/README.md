# Kafka Scripts

Utility scripts for producing and consuming Kafka messages.

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Kafka:**
   - Update `../../config/kafka/client.properties` with your Kafka credentials
   - Or place a `client.properties` file in this directory

## Usage

### Producer

Send events from a JSON file to Kafka:

```bash
# Use default file (data.json) and default topic (event)
python producer.py

# Specify custom file path
python producer.py data.json

# Specify file path and topic
python producer.py data.json event
```

### Consumer

Receive events from Kafka:

```bash
# Consume from default topic (event), unlimited messages
python consumer.py

# Consume from specific topic
python consumer.py event

# Consume limited number of messages
python consumer.py event 10

# Consume and save to JSON file
python consumer.py event 100 output.json
```

## Configuration

The scripts automatically look for configuration in:
1. `../../config/kafka/client.properties` (project config)
2. `./client.properties` (local fallback)

## Examples

```bash
# Send events from data.json to event topic
python producer.py data.json event

# Receive first 10 messages and save to output.json
python consumer.py event 10 output.json
```

