from prometheus_metrics import (
    start_prometheus_server,
    record_consumption_error,
    record_message_consumed,
    record_partition_eof,
    record_consumption_latency
)
from confluent_kafka import Consumer, KafkaException, KafkaError
from tenants.tenant import Tenant  
import sys
import importlib.util
import time
import logging


# Start Prometheus metrics server
start_prometheus_server(8000)


class TenantStream(Tenant):
    def __init__(self, tenant_id, kafka_servers='localhost:9092'):
        # Initialize the parent Tenant class
        super().__init__(tenant_id)
        self.kafka_servers = kafka_servers
        self.consumer = self.create_consumer(f"{tenant_id}_topic", f"{tenant_id}_groupid")
        self.logger_stream = self._setup_stream_logger()
        self.is_running = False  # Flag to control the pipeline state

    
    def _setup_stream_logger(self):
        """Set up a logger specifically for batch operations."""
        log_dir_stream = self.log_dir / "stream"
        
        if not log_dir_stream.exists():
            log_dir_stream.mkdir(parents=True, exist_ok=True)
        
        # Set up logger for batch processes
        logger = logging.getLogger(f"{self.tenant_id}_stream")
        logger.setLevel(logging.INFO)  # Set to appropriate level

        log_file = log_dir_stream / f'{self.tenant_id}_stream.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger



    def create_consumer(self, topic, group_id):
        """Create a Kafka consumer for the tenant's topic."""
        consumer_config = {
            'bootstrap.servers': self.kafka_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # Start from the earliest offset
            'enable.auto.commit': False,  # Disable auto-commit
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])  # Subscribe to the topic
        return consumer

    def consume_data(self, topic):
        """Consume data from Kafka and return the message."""
        try:
            start_time = time.time()  # Start timing
            msg = self.consumer.poll(timeout=1)
            if msg is None:
                self.logger_stream.info(f"No new messages in topic: {topic}")
                return None

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    record_partition_eof()  # Record metric for partition EOF
                    self.logger_stream.info(f"End of partition reached: {msg.topic()} [{msg.partition}] at offset {msg.offset()}")
                else:
                    record_consumption_error()  # Record metric for errors
                    self.logger_stream.error(f"Error consuming message: {msg.error()}")
                return None

            # Successfully consumed a message
            message_value = msg.value().decode('utf-8')
            record_message_consumed()  # Record metric for successful consumption
            record_consumption_latency(time.time() - start_time)  # Record latency
            return message_value  # Return the message value for processing

        except KafkaException as e:
            record_consumption_error()
            self.logger_stream.error(f"Kafka error consuming message from {topic}: {e}")
            return None

        except Exception as e:
            record_consumption_error()
            self.logger_stream.error(f"Unexpected error while consuming data from {topic}: {e}")
            return None

    def stream_ingest(self):
        """
        Dynamically import and run the stream ingestion pipeline for the tenant.
        This method loads {tenant_id}_streamingestpipeline.py and calls the functions within it.
        """
        try:
            pipeline_file_path = self.tenant_dir / f"{self.tenant_id}_streamingestpipeline.py"

            if pipeline_file_path.exists():
                # Add the directory containing the pipeline script to sys.path
                sys.path.append(str(self.tenant_dir))

                # Dynamically load the tenant-specific pipeline script
                module_name = f"{self.tenant_id}_streamingestpipeline"
                spec = importlib.util.spec_from_file_location(module_name, str(pipeline_file_path))
                pipeline_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(pipeline_module)
                
                # Run the pipeline's main function
                pipeline_module.main(self)
                
                # After running the pipeline, log success
                self.logger_stream.info(f"METRICS - {self.tenant_id} - Successfully ingested data for tenant.")



                # Call the prepare_data_for_ingestion method from the tenant's pipeline
                

                self.logger_stream.info(f"Successfully ingested data for tenant: '{self.tenant_id}'")
            else:
                self.logger_stream.error(f"Pipeline file not found: {pipeline_file_path}")
                raise FileNotFoundError(f"Pipeline file for tenant '{self.tenant_id}' not found.")

        except Exception as e:
            self.logger_stream.error(f"Error executing stream ingestion pipeline for tenant '{self.tenant_id}': {e}")
            raise

    def start_pipeline(self):
        """Start the streaming ingestion pipeline."""
        if not self.is_running:
            self.is_running = True
            self.logger_stream.info(f"Started streaming pipeline for tenant {self.tenant_id}.")
            # Simulate continuous consumption and processing of messages
            while self.is_running:
                message = self.consume_data("tenant1_topic")
                if message:
                    self.stream_ingest()  # Process the message
                time.sleep(1)  # Sleep to control the flow of data consumption
        else:
            self.logger_stream.warning(f"Pipeline for tenant {self.tenant_id} is already running.")

    def stop_pipeline(self):
        """Stop the streaming ingestion pipeline."""
        if self.is_running:
            self.is_running = False
            self.logger_stream.info(f"Stopped streaming pipeline for tenant {self.tenant_id}.")
        else:
            self.logger_stream.warning(f"Pipeline for tenant {self.tenant_id} is not running.")
