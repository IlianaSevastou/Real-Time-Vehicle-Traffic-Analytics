  GNU nano 7.2                                                          kafka_consumer.py
from kafka import KafkaConsumer
import json
import os
import time

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "vehicle-positions")
KAFKA_API_VERSION = (3, 9, 0)

# Create a Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    auto_offset_reset='earliest',  # Start reading from the beginning
    enable_auto_commit=True,  # Automatically commit the offsets
    group_id='vehicle-positions-group',  # Consumer group
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Deserialize JSON messages
    api_version=KAFKA_API_VERSION
)

print(f"Kafka Consumer connected to topic: {KAFKA_TOPIC}")
print("Listening for messages...")


# Consume messages
for message in consumer:
      print("Received Message:")
      print(json.dumps(message.value, indent=4))



consumer.close()
print("Kafka Consumer closed.")
