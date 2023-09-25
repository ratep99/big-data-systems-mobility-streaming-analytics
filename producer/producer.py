import time
import os
import csv
import json
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=[os.environ["KAFKA_HOST"]],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(0, 11),
)

while True:
    with open(os.environ["DATA"], "r") as file:
        reader = csv.reader(file, delimiter=",")
        headers = next(reader)
        for row in reader:
            value = {
                "timestamp": row[0],
                "id": row[1],
                "type": row[2],
                "latitude": float(row[3]),
                "longitude": float(row[4]),
                "speed_kmh": float(row[5]),
                "acceleration": float(row[6]),
                "distance": float(row[7]),
                "odometer": float(row[8]),
                "pos": float(row[9])
            }
            value["ts"] = int(time.time())
            producer.send(os.environ["KAFKA_TOPIC"], value=value)
            time.sleep(float(os.environ["KAFKA_INTERVAL"]))