import yaml
from pathlib import Path
from tenants.tenant_stream import TenantStream  
from coredms.core_dms import CoreDMS

def get_all_tenants():
    tenant_yaml_path = Path(__file__).parent.parent / 'tenants' / 'tenants.yaml'
    print(f"Attempting to load tenants.yaml from: {tenant_yaml_path}")
    
    try:
        with tenant_yaml_path.open('r') as file:
            data = yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Error: File {tenant_yaml_path} not found!")
        return []
    except yaml.YAMLError as e:
        print(f"Error reading YAML file: {e}")
        return []
    
    tenants = data.get('tenants', [])
    if not tenants:
        print("No tenants found in the YAML file.")
    return tenants

# Manager class to control the start and stop of tenant pipelines
class StreamIngestManager:
    def __init__(self):
        self.tenants = []
        self.core_dms = CoreDMS()

    def load_tenant(self, tenant_id):
        tenant_stream = TenantStream(tenant_id)
        return tenant_stream

    def start_tenant_pipeline(self, tenant_id):
        tenant_stream = self.load_tenant(tenant_id)
        tenant_stream.start_pipeline()

    def stop_tenant_pipeline(self, tenant_id):
        tenant_stream = self.load_tenant(tenant_id)
        tenant_stream.stop_pipeline()

def main():
    manager = StreamIngestManager()
    tenants = get_all_tenants()

    if not tenants:
        print("No tenants to process. Exiting.")
        return

    # Start pipelines for all tenants
    for tenant_id in tenants:
        try:
            manager.start_tenant_pipeline(tenant_id)

        except Exception as e:
            print(f"Error while starting streaming for '{tenant_id}': {e}")


if __name__ == "__main__":
    main()
