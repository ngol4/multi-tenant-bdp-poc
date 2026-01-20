import polars as pl
from pathlib import Path
import logging
from confluent_kafka import Producer
import json
import random
import time
from prometheus_metrics import (
    record_message_sent,
    record_successful_delivery,
    record_delivery_latency,
    record_total_ingested_size,
    update_average_ingestion_time,
    start_prometheus_server
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
DATA_DIR = "data/azurefunctions-dataset2019"
KAFKA_SERVERS = 'localhost:9092'

# Initialize Kafka producer
conf = {
    'bootstrap.servers': KAFKA_SERVERS,
    'client.id': 'csv_producer',
    'queue.buffering.max.messages': 1000000
}
producer = Producer(conf)

# Track total ingestion time and message count
total_ingestion_time = 0
message_count = 0

def delivery_report(err, msg, start_time):
    """
    Callback function to handle delivery reports.
    """
    global message_count
    end_time = time.time()
    delivery_latency = end_time - start_time  # Calculate delivery latency
    
    # Record the delivery latency
    record_delivery_latency(delivery_latency)  # Track the latency metric

    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        record_successful_delivery()  # Record successful delivery metric
        message_count += 1
        if message_count % 100 == 0:
            logger.info(f"Successfully delivered {message_count} messages.")

def send_message(topic, message):
    """
    Send a message to Kafka and track metrics.
    """
    global total_ingestion_time

    start_time = time.time()  # Track start time
    msg_value = json.dumps(message)
    
    # Send message to Kafka
    producer.produce(topic, value=msg_value, callback=lambda err, msg: delivery_report(err, msg, start_time))
    producer.poll(10)

    ingestion_time = time.time() - start_time  # Calculate ingestion time
    total_ingestion_time += ingestion_time

    # Track message count
    record_message_sent()  # Increment messages sent counter

    # Record total ingested size in bytes
    record_total_ingested_size(len(msg_value.encode('utf-8')))
    # Update the average ingestion time
    update_average_ingestion_time(total_ingestion_time, message_count)

def stream_csv_to_kafka():
    for i in range(1, 15):
        file_name = f"function_durations_percentiles.anon.d{str(i).zfill(2)}.csv"
        file_path = Path(__file__).parent.parent.parent / DATA_DIR / file_name
        logger.info(f"Checking file path: {file_path}")

        if file_path.exists():
            logger.info(f"Processing file: {file_path}")
            try:
                df = pl.read_csv(file_path)
                logger.info(f"Read DataFrame with {df.height} rows.")

                for row in df.iter_rows():
                    message = {df.columns[j]: row[j] for j in range(len(df.columns))}
                    topic = random.choice(['tenant1_topic', 'tenant2_topic'])
                    send_message(topic, message)

            except Exception as e:
                logger.error(f"Error reading file {file_path}: {e}")
        else:
            logger.warning(f"File not found: {file_path}")

def main():
    start_prometheus_server(8000)  # Start Prometheus metrics server
    stream_csv_to_kafka()
    producer.flush()

if __name__ == '__main__':
    main()
