import logging
from pathlib import Path
import yaml

class Tenant:
    def __init__(self, tenant_id):
        # Initialize the tenant with provided tenant_id and core_dms.
        self.tenant_id = tenant_id
        self.tenant_dir = Path(__file__).parent / tenant_id  # Ensure this points to the correct directory
        self.log_dir = self.tenant_dir / 'logging' 



    def load_config(self):
            """
            Loads the tenant's configuration from the config.yaml file.
            
            Returns:
                dict: The configuration dictionary containing tenant-specific settings.
            """
            config_file = self.tenant_dir / f'{self.tenant_id}_config.yaml'  # Assume config.yaml is stored in the tenant directory
            try:
                if config_file.exists():
                    with config_file.open('r') as file:
                        config = yaml.safe_load(file)
                    #self.logger.info(f"BATCH - {self.tenant_id} -  Configuration loaded.")
                    return config
                else:
                    #self.logger.warning(f"BATCH - {self.tenant_id} - Configuration file not found")
                    return {}  # Return empty dict if the config file is not found
            except Exception as e:
                #self.logger.error(f"Error loading config for tenant '{self.tenant_id}': {e}")
                return {}  # Return empty dict if an error occurs while loading the config