import os
import time
import random
import json
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Configuration for Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "vehicle-positions")
KAFKA_API_VERSION = (3, 9, 0)

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    api_version=KAFKA_API_VERSION,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load Simulation Data
data_file = "uxsim_data.csv"
try:
    df = pd.read_csv(data_file)
    print(f"Loaded {len(df)} records from {data_file}")
except FileNotFoundError:
    print(f"Error: File {data_file} not found!")
    exit(1)

produser_start_time = datetime.now()

# Send Data to Kafka Topic
for index, row in df.iterrows():
    #calculate message send time based on column t
    send_time = produser_start_time + timedelta(seconds=row['t'])

     #wait until the calculated sent time
    while datetime.now() < send_time:
          time.sleep(0.1)

    #check if the vehicle is in 'waiting_at_origin_mode', skip if true
    if row['x'] == -1.0 and row['s'] == -1.0 and row['v'] == -1.0:
           continue

    message = {
        "name": str(row['name']),
        "origin": row['orig'],
        "destination": row['dest'],
        "time": send_time.strftime("%Y-%m-%d %H:%M:%S"),
        "link": row['link'],
        "position": row['x'],
        "spacing": row['s'],
        "speed": row['v']
    }
    print(f"Sending message: {message}")
    producer.send(KAFKA_TOPIC, message)

# Flush and Close Producer
producer.flush()
producer.close()
print("All messages sent successfully to Kafka!")