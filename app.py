from dotenv import load_dotenv
import boto3
import json
import os
import psycopg2
import time

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
SQS_URL = os.getenv("SQS_URL")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

sqs = boto3.client('sqs', region_name=AWS_REGION)

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS home_sensor_events (
    id SERIAL PRIMARY KEY,
    sensor_name VARCHAR(50),
    event VARCHAR(50),
    timestamp BIGINT
)
""")
conn.commit()

print("Processor started. Listening for messages...")

while True:
    try:
        response = sqs.receive_message(
            QueueUrl=SQS_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10
        )

        messages = response.get('Messages', [])

        for msg in messages:
            data = json.loads(msg['Body'])
            print("Received message. Beginning data processing for:", data)

            cursor.execute(
                "INSERT INTO home_sensor_events (sensor_name, event, timestamp) VALUES (%s, %s, %s)",
                (data["sensor_name"], data["event"], data["timestamp"])
            )
            conn.commit()

            sqs.delete_message(
                QueueUrl=SQS_URL,
                ReceiptHandle=msg['ReceiptHandle']
            )

    except Exception as e:
        print("Error occured. Exception:", e)
