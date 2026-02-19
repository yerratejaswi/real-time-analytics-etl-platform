from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    event = {
        "user_id": random.randint(1, 1000),
        "event_type": random.choice(["login", "video_view", "like", "share"]),
        "watch_time": random.randint(5, 300),
        "timestamp": int(time.time())
    }
    producer.send("user_events", event)
    print("Sent:", event)
    time.sleep(1)
