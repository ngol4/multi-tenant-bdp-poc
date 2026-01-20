import sys
import importlib.util
from tenants.tenant import Tenant  # Assuming Tenant class is in tenant.py or in the same directory
import time
from coredms.core_dms import CoreDMS
import json
import logging
import datetime


core_dms = CoreDMS()

class TenantBatch(Tenant):
    def __init__(self, tenant_id):
        # Call the parent constructor (Tenant)
        super().__init__(tenant_id)
        self.metrics_index = f"{tenant_id}_metrics"
        self.metrics = {}
        # Setting up different loggers for batch and stream
        self.logger_batch = self._setup_batch_logger()

    def _setup_batch_logger(self):
        """Set up a logger specifically for batch operations."""
        log_dir_batch = self.log_dir / "batch"
        
        if not log_dir_batch.exists():
            log_dir_batch.mkdir(parents=True, exist_ok=True)
        
        # Set up logger for batch processes
        logger = logging.getLogger(f"{self.tenant_id}_batch")
        logger.setLevel(logging.INFO)  # Set to appropriate level

        # File handler for batch log
        log_file = log_dir_batch / f'{self.tenant_id}_batch.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Console handler for batch logs as well
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Set up formatter and add handlers to the logger
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger
    
    def get_schedule_interval(self):
        """
        Retrieves the schedule interval for the batch ingestion of the current tenant.
        Uses the load_config method from Tenant to load tenant-specific config.
        """
        config = self.load_config()
        schedule_interval = config.get('schedule_interval', "*/2 * * * *")  # Default to every 2 minutes if not found
        return schedule_interval
    
    def batch_ingest(self):
        """
        Perform batch ingestion to Elasticsearch using the internal CoreDMS instance
        and pipeline logic defined in the tenant-specific pipeline file.
        """
        self.logger_batch.info(f"Starting batch ingestion for tenant: '{self.tenant_id}'")

        # Start time for metrics tracking
        start_time = time.time()
        self.metrics["pipeline_start"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


        # Dynamically load and execute the tenant's batch ingestion pipeline
        self.run_batch_pipeline()

        # Log METRICS data after pipeline execution
        ingestion_time = time.time() - start_time
        self.metrics["ingestion_time"] = ingestion_time
        self.metrics["pipeline_end"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.collect_and_insert_metrics()

        self.logger_batch.info(f"METRICS-Ingestion completed in {ingestion_time:.4f} seconds for tenant: {self.tenant_id}")
        self.logger_batch.info(f"METRICS-Tenant {self.tenant_id} batch ingestion complete.")
        

    

    def run_batch_pipeline(self):
        """
        Dynamically import and run the batch ingestion pipeline for the tenant.
        This method loads the {tenant_id}_batchingestpipeline.py file and calls the functions within it.
        """
        try:
            # Define the path to the batch ingestion pipeline file
            pipeline_file_path = self.tenant_dir / f"{self.tenant_id}_batchingestpipeline.py"
        
            if pipeline_file_path.exists():
                # Add the directory containing the pipeline script to sys.path
                sys.path.append(str(self.tenant_dir))
                
                # Dynamically load the tenant-specific pipeline script
                module_name = f"{self.tenant_id}_batchingestpipeline"
                spec = importlib.util.spec_from_file_location(module_name, str(pipeline_file_path))
                pipeline_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(pipeline_module)
                
                # Run the pipeline's main function
                pipeline_module.main(self)
                
                # After running the pipeline, log success
                self.logger_batch.info(f"METRICS - {self.tenant_id} - Successfully ingested data for tenant.")


            else:
                # Log error if the pipeline file doesn't exist
                self.logger_batch.error(f"BATCH - {self.tenant_id} - Pipeline file not found: {pipeline_file_path}")
                raise FileNotFoundError(f"BATCH - {self.tenant_id} - Pipeline file not found: {pipeline_file_path}")

        except Exception as e:
            # Log any error that occurs during the pipeline execution
            self.logger_batch.error(f"METRICS - {self.tenant_id} - Error in executing the batch pipeline for tenant: {e}")
            raise


    def collect_and_insert_metrics(self):
        """
        Collect metrics after the batch ingestion and insert them into CoreDMS.
        """
        # Collect the necessary metrics (example)
        #data_size = self.get_data_size()  # Method to get data size, you should define this
        
        # Log the metrics in JSON format
        metrics_json = json.dumps(self.metrics)
        self.logger_batch.info(f"METRICS data collected for tenant {self.tenant_id}: {metrics_json}")
        
        # Insert metrics into CoreDMS (assuming insert method is available in CoreDMS class)
        core_dms.insert(self.metrics, self.metrics_index)  # Insert metrics into the index
        
        # Optionally, you can log the insertion process if required
        self.logger_batch.info(f"Metrics inserted into CoreDMS for tenant {self.tenant_id}")
