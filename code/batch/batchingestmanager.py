import yaml
from pathlib import Path
from tenants.tenant_batch import TenantBatch  # Import TenantBatch instead of Tenant
from coredms.core_dms import CoreDMS
from apscheduler.schedulers.blocking import BlockingScheduler
import time


class BatchIngestManager:

    def __init__(self):
        self.tenants_batch_instances = []
        self.scheduler = BlockingScheduler()

    def get_all_tenants(self):
        """
        Retrieve all tenants from the tenants.yaml file.
        """
        tenant_yaml_path = Path(__file__).parent.parent / 'tenants' / 'tenants.yaml'
        with tenant_yaml_path.open('r') as file:
            data = yaml.safe_load(file)
        tenants = data.get('tenants', [])
        return tenants

    def add_tenants(self, tenants):
        """
        Add tenant instances to the BatchIngestManager.
        """
        for tenant in tenants:
            tenant_batch_instance = TenantBatch(tenant)
            self.tenants_batch_instances.append(tenant_batch_instance)

    def batch_ingest_for_tenant(self, tenant_batch_instance):
        """
        Perform batch ingestion for a specific tenant instance, considering the interval for scheduling.
        """
        start_time = time.time()

        # Perform the batch ingestion method from TenantBatch
        tenant_batch_instance.batch_ingest()

        end_time = time.time()
        print(f"Time taken to perform batch ingestion for tenant '{tenant_batch_instance.tenant_id}': {end_time - start_time:.6f} seconds")

    def ingest_based_on_schedule(self, tenant_batch_instance):
        """
        Ingest data based on the tenant's defined schedule (cron expression).
        This function checks the schedule and ensures ingestion happens at the right time.
        """
        cron_expression = tenant_batch_instance.get_schedule_interval()  # Get cron expression from tenant

        # If there's a cron expression, set up the ingestion on the schedule
        self.scheduler.add_job(self.scheduled_task_for_tenant, 'cron', **self.parse_cron_expression(cron_expression), args=[tenant_batch_instance])

    def scheduled_task_for_tenant(self, tenant_batch_instance):
        """
        Perform batch ingestion for a single tenant based on its scheduled interval.
        """
        self.batch_ingest_for_tenant(tenant_batch_instance)

    def parse_cron_expression(self, cron_expression):
        """
        Parse the cron expression into a dictionary format that APScheduler can understand.
        """
        # Split the cron expression into its components
        cron_parts = cron_expression.split()

        # Return as a dictionary of arguments
        return {
            'minute': cron_parts[0],
            'hour': cron_parts[1],
            'day': cron_parts[2],
            'month': cron_parts[3],
            'day_of_week': cron_parts[4],
        }


if __name__ == "__main__":
    # Initialize the BatchIngestManager
    core_dms = CoreDMS()
    manager = BatchIngestManager()

    # Get and add all tenants to the manager
    tenants = manager.get_all_tenants()
    manager.add_tenants(tenants)


    # For each tenant, schedule their ingestion based on their defined cron expression
    for tenant_batch_instance in manager.tenants_batch_instances:
        manager.ingest_based_on_schedule(tenant_batch_instance)

    print("Scheduler started with dynamic cron-based scheduling...")

    # Start the scheduler
    manager.scheduler.start()
