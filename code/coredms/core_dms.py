import logging
from elasticsearch import Elasticsearch, helpers
import time

# Configure logging
logging.basicConfig(level=logging.INFO,  
                    format='%(asctime)s - %(levelname)s - %(message)s')

def wait_for_elasticsearch():
    """
    Wait until Elasticsearch is available.
    """
    while True:
        try:
            es = Elasticsearch([
                "http://localhost:9200",
                "http://localhost:9201",
                "http://localhost:9202",

            ])
            if es.ping():
                logging.info("Connected to Elasticsearch!")
                return es
        except Exception as e:
            logging.warning(f"Waiting for Elasticsearch... {e}")
            time.sleep(10)  # Retry every 10 seconds


class CoreDMS:
    def __init__(self, shards=4, replicas=2):
        """
        CoreDMS connects to Elasticsearch and provides index management and data storage.
        :param shards: Number of primary shards (default: 4)
        :param replicas: Number of replica shards (default: 2)
        """
        self.es = wait_for_elasticsearch()
        self.default_shards = shards
        self.default_replicas = replicas

    def create_index(self, index_name, mapping=None):
        """
        Create an index in Elasticsearch with the given settings and mappings.
        :param index_name: The name of the index to create.
        :param mapping: Optional; A dictionary defining field mappings.
        """
        try:
            if not self.es.indices.exists(index=index_name):
                settings = {
                    "settings": {
                        "number_of_shards": self.default_shards,
                        "number_of_replicas": self.default_replicas
                    }
                }
                if mapping:
                    settings["mappings"] = mapping

                self.es.indices.create(index=index_name, body=settings)
                logging.info(f"Index '{index_name}' created with {self.default_shards} shards and {self.default_replicas} replicas.")
            else:
                logging.info(f"Index '{index_name}' already exists.")
        except Exception as e:
            logging.error(f"Error creating index '{index_name}': {e}")

    def store_data(self, index_name, data, partition_key=None):
        """ 
        Store data in Elasticsearch with optional partitioning.
        :param index_name: The index where data should be stored.
        :param data: List of documents (dictionaries).
        :param partition_key: Optional key for routing (e.g., 'app' field).
        """
        if not isinstance(data, list):  # Ensure data is a list
            data = [data]

        actions = [
            {
                "_index": index_name,
                "_source": doc,
                "routing": str(doc[partition_key]) if partition_key and partition_key in doc else None
            }
            for doc in data
        ]

        if actions:
            try:
                helpers.bulk(self.es, actions,  chunk_size=8000)
                logging.info(f"Successfully stored {len(actions)} documents in '{index_name}'.")
            except Exception as e:
                logging.error(f"Error storing data in index '{index_name}': {e}")

    def insert(self, document, index_name):
        """
        Insert a single document into Elasticsearch.
        :param document: The document (dictionary) to insert.
        :param doc_id: Optional; The ID of the document. If not provided, Elasticsearch will generate one.
        """
        try:
            response = self.es.index(index=index_name, document=document)
            logging.info(f"Document inserted successfully: {response['_id']}")
        except Exception as e:
            logging.error(f"Error inserting document: {e}")


