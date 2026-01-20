import time
from coredms.core_dms import CoreDMS 
from tenants.tenant_stream import TenantStream  
INDEX_NAME = "tenant1_streamtest"
core_dms = CoreDMS()


def main(tenant:TenantStream):
    """Consume messages from 'tenant1_topic', apply wrangling, and store them in core_dms."""
    
    core_dms.create_index(INDEX_NAME)
    while True:
        try:
            # Continuously consume a message from Kafka
            message_value = tenant.consume_data("tenant1_topic")

            if message_value:
                #tenant.logger_stream.info(f"Consumed message: {message_value}")

                # Wrangling: Perform any processing on the message
                processed_message = process_message(message_value, tenant.logger_stream)

                # Insert the processed message into core_dms immediately
                core_dms.insert(processed_message, INDEX_NAME)
                #tenant.logger_stream.info("Message stored in core_dms.")
            
            # No need for additional delays here. The loop runs continuously as long as Kafka messages are available.
            # You may add a small sleep here if you want to control the rate of consumption
            # time.sleep(0.1)  # Optional, not necessary for continuous processing.

        except KeyboardInterrupt:
            # Gracefully handle script termination
            tenant.logger_stream.info("Data ingestion interrupted. Shutting down.")
            break

        except Exception as e:
            # Handle unexpected errors
            tenant.logger_stream.error(f"Unexpected error: {e}")
            # Sleep before retrying in case of an error to avoid tight error loops
            time.sleep(5)

def process_message(message_value, logger_stream):
    """Wrangle the message (tenant-specific logic)."""
    # Example of a wrangling operation: convert message to uppercase
    logger_stream.info(f"Processing message: {message_value}")
    processed_message = message_value.lower()  # Example processing step
    return processed_message



if __name__ == "__main__":
    tenant = TenantStream(tenant_id="tenant2") 
    main(tenant)

