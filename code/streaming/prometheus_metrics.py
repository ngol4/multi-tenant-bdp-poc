from prometheus_client import Counter, Summary, Gauge, start_http_server
import time

# Initialize Prometheus metrics for Consumer
messages_consumed = Counter('kafka_messages_consumed_total', 'Total number of messages consumed')
consumption_errors = Counter('kafka_consumption_errors_total', 'Total number of consumption errors')
consumption_latency = Summary('kafka_consumption_latency_seconds', 'Time taken to consume a message')
partition_eof = Counter('kafka_partition_eof_total', 'Total number of end-of-partition events')

# Initialize Prometheus metrics for Kafka producer
messages_sent = Counter('kafka_messages_sent_total', 'Total number of messages sent to Kafka')
successful_deliveries = Counter('kafka_messages_delivered_successful', 'Number of successfully delivered messages')
delivery_latency = Summary('kafka_message_delivery_latency_seconds', 'Latency of Kafka message delivery')
total_ingested_size = Counter('kafka_total_ingested_data_size_bytes', 'Total size of ingested data in bytes')
message_size = Summary('kafka_message_size_bytes', 'Size of each Kafka message')

# Use Gauge for average_ingestion_time since we are setting its value directly
average_ingestion_time = Gauge('kafka_average_ingestion_time_seconds', 'Average ingestion time per message')

def start_prometheus_server(port=8000):
    """
    Start the Prometheus HTTP server to expose metrics.
    """
    start_http_server(port)
    print(f"Prometheus metrics exposed on port {port}")

def record_consumption_latency(start_time):
    """
    Record the consumption latency for a message.
    """
    consumption_latency.observe(time.time() - start_time)

def record_consumption_error():
    """
    Increment the consumption error counter.
    """
    consumption_errors.inc()

def record_partition_eof():
    """
    Increment the partition EOF counter.
    """
    partition_eof.inc()

def record_message_consumed():
    """
    Increment the message consumed counter.
    """
    messages_consumed.inc()

def record_delivery_latency(start_time):
    """
    Record the delivery latency for a message.
    """
    delivery_latency.observe(time.time() - start_time)

def record_message_sent():
    """
    Increment the message sent counter.
    """
    messages_sent.inc()

def record_successful_delivery():
    """
    Increment the successful delivery counter.
    """
    successful_deliveries.inc()

def record_total_ingested_size(size):
    """
    Increment the counter for the total ingested size in bytes.
    """
    total_ingested_size.inc(size)

def record_message_size(size):
    """
    Observe the size of each message.
    """
    message_size.observe(size)

def update_average_ingestion_time(total_time, message_count):
    """
    Update the average ingestion time based on the total time and message count.
    """
    if message_count > 0:
        # Set the average ingestion time using the Gauge metric
        average_ingestion_time.set(total_time / message_count)
