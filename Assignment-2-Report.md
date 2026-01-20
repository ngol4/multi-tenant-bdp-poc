

## Multi-tenancy model

### Tenant Registration in MySIMBDP

Upon registering into ```mysimbdp```, each tenant will be added to the registry. In this implementation, the registry is mocked as:

```
code/tenants/tenants.yaml
```

This file contains the ID of each registered tenant.

### Tenant Directory Structure
Each tenant has a corresponding folder to ensure that each tenant has an isolated configuration and ingestion pipeline within MySIMBDP, which is under:

```
code/tenants/<tenant_id>
```

Within this folder, the following files are generated:

```
code/tenants/<tenant.id>/
├── <tenantID>_batchingestpipeline.py
├── <tenantID>_config.yaml
├── <tenantID>_streamingingestpipeline.py

```

The information or meta data about each tenant, including details such as tenant_id, directory path, configuration file, and logger) are encapsulated in class ```Tenant``` and located in ```code/tenants/tenant.py```). This class serves as the foundational structure for managing tenant-specific attributes and behaviors. The ```TenantBatch``` class (```code/tenants/tenant_batch.py```), resposnible for handling batch data processing pipelines, and ```TenantStream``` class (```code/tenants/tenant_stream.py```), which manages real-time or near real-time data ingestion, both extend the ```Tenant``` class. By doing so,  they inherits its properties and methods while adding specialized functionalities tailored for batch and streaming ingestion, respectively. 




# Part 1 - Batch data ingestion and transformation 


> 1. The ingestion and transformation will be applied to les of data. Design a schema for a set of
constraints for data les that mysimbdp will support the ingestion and transformation. Design a
schema for a set of constraints for tenant service agreement. Explain why you, as a platform provider,
decide and use such constraints. Implement these constraints into simple conguration les. Provide
two dierent examples (e.g., JSON or YAML) for two dierent tenants to specify constraints on
service agreement and files for the tenant and explain why such constraints are suitable for the
tenants. (1 point)
The constraints are defined in the YAML file in ```<tenantID>_config.yaml```.

#### Service Agreement Constraints:

- Max Concurrent Ingestions: Limits the number of simultaneous data ingestion jobs a tenant can trigger to avoid resource contention.
- ngestion Frequency: Ensures that the tenant doesn’t overload the system by uploading data too frequently (e.g., once a day).
- Priority: If the system is under heavy load, higher priority tenants may get preferential treatment.
- SLA (Service Level Agreement): Ensures that data ingestion and processing meet the required time limits, with penalties or compensations for missed SLAs.


### Data File Constraints:

- File Size Limit: The maximum file size ensures that large files don’t negatively affect the platform’s performance.
- Allowed File Types: Only certain file formats (e.g., CSV, JSON) are supported for ingestion.


> 2. Each tenant will put the tenant's data files to be ingested and transformed into a staging le directory
(or a storage bucket), tenant-staging-input-dir within mysimbdp (the staging directory is managed by
the platform). Each tenant provides its ingestion and transformation pipelines, batchingestpipeline,
which will take the tenant's les as input, in tenant-staging-input-dir, and ingest and transform data
in the les into mysimbdp-coredms. Any batchingestpipeline must perform at least one type of data
wrangling to transform data elements in les to another structure during ingestion. As a tenant,
explain the design of batchingestpipeline and provide one implementation. Note that
batchingestpipeline follows the guideline of mysimbdp given in the next Point 3. (1 point)


For ease, all the data input is under folder ```data```. Every tenant will write their pipeline intp ```<tenant_id>_batchingestpipeline.py``` file in their respective folfer. The pipeline has to follow the model imposed which includes the main function that ```BatchIngestManager``` will call , which is describe in part 3. 

The tenant would read from the file, transform, and ingest it to Elastic search by calling the methods of Elasticsearch.  In the tenant-specific configuration file, the tenant will provide the schedule for their batch pipeline in cron-based expressions so that the ```BatchIngestManager``` knows when to trigger the pipeline. 


> 3. The mysimbdp provider provisions an execution environment for running tenant's ingestion and
transformation pipelines (batchingestpipeline). As the mysimbdp provider, design and implement a
component mysimbdp-batchingestmanager that invokes tenant's batchingestpipeline to perform
the ingestion/transformation for available les in tenant-staging-input-dir. mysimbdp imposes the
model that batchingestpipeline has to follow but batchingestpipeline is, in principle, a blackbox to
mysimbdp-batchingestmanager. Explain how mysimbdp-batchingestmanager knows the list of
batchingestpipeline and decides/schedules the execution of batchingestpipeline for dierent
tenants. (1 point)


The ```BatchIngestManager``` is responsible for invoking tenant-specific batching ingestion and transformation pipelines.

#### 1. Tenant Discovery and Pipeline Identification

To determine which batchingestpipeline to execute for each tenant, the ```BatchIngestManager``` performs the following steps:

- Tenant Configuration: The platform maintains a central configuration file (```code/tenants/tenants.yaml```) that lists all tenants, including their unique tenant_id. Through the unique_id, more information such as their directory path can be easily retrieved.

- Pipeline Discovery: The ```BatchIngestManager``` scans the designated directory for each tenant and searching for a pipeline script with the naming convention ```<tenant_id>_batchingestpipeline.py```. This dynamic discovery allows for easy addition or removal of tenant pipelines without modifying the core system.

- Modular model imposed: Each ```<tenant_id>_batchingestpipeline.py``` script must conform to a standard model set by ```mysimbdp```, with  the inclusion of a required function ```main()```. By default, the ```BatchIngestManager``` invokes the ```main()``` function in each tenant-specific pipeline, which handles all the steps in tenant's pipeline.  Despite adhering to this common model, each pipeline remains a black box for the ```BatchIngestManager```, which enables customizations tailored to individual tenants.


#### 2. Execution and Scheduling of Pipelines
Once the ```BatchIngestManager``` has discovered the appropriate pipeline for each tenant, it schedules and invokes these pipelines based on various criteria, including tenant-specific configurations.

- Pipeline Execution: The ```BatchIngestManager``` will not require knowledge of the specific details of each tenant’s pipeline but instead, it will dynamically load and execute the pipeline script using Python's ```importlib``` module. This enables the platform to work with new tenants and pipelines without requiring code changes.

- Scheduling Logic: The system retrieves the scheduling interval from a configuration file of each tenant, which is deault in cron-based expression. The ```BatchIngestManager``` is integrated with the ```APScheduler (Advanced Python Scheduler)``` framework to ensure the pipelines are triggered automatically at the appropriate times based on the tenant's defined schedule.


#### 3. Isolation and Flexibility
- Tenant Isolation: Each tenant's pipeline runs in isolation and concurrently to ensure that the execution of one tenant’s pipeline does not interfere with the others. This isolation is particularly important when dealing with multi-tenancy scenarios, where data security and privacy are critical.

- Flexibility for New Tenants: The ```BatchIngestManager``` is designed to be scalable. New tenants can be added without modifying the core system by simply adding their correspondiung configuration files and scripts following the prescribed model. 



> 4. Explain your design for the multi-tenancy model in mysimbdp: which parts of mysimbdp will be
shared for all tenants, which parts will be dedicated for individual tenants so that you, as a platform
provider, can add and remove tenants based on the principle of pay-per-use. Develop test
batchingestpipeline, test data, and test constraints of les, and test service proles for tenants
according to your deployment. Show the performance of ingestion tests, including failures and
exceptions, for 2 dierent tenants in your test environment and constraints. Demonstrate examples
in which data will not be ingested due to a violation of constraints. Present and discuss the maximum
amount of data per second you can ingest in your tests. (1 point)


In ```mysimbdp```, the multi-tenancy model is designed to balance shared resources and tenant-specific components, enabling efficient management and scalability based on the pay-per-use principle.

### Shared Components
-  Core Data Management System (```CoreDMS```) under directory ``` code/coredms```: 
The core DMS is structured as an Elasticsearch cluster consisting of three nodes. This cluster serves as the central repository for all ingested and transformed data across multiple tenants. 

- Batch Ingestion Manager (```BatchIngestionManager```) under directory ```code/batch/batchingestmanager.py```:  The batch ingestion manager serves as controller to orchestrate the execution of batch ingestion pipelines for all tenants, monitoring directories for incoming data files and invoking the appropriate tenant-specific pipelines.



- Monitoring and Logging System (mysimbdp-monitoring): Collects and analyzes logs and metrics from ingestion processes, offering insights into performance, failures, and data quality across the platform.

### Tenant-Specific Components
- Tenant-speicific congfiguration file (``code/tenants/<tenant_id>/<tenant_id>_config.yaml``): Each tenant has dedicated configuration files specifying constraints on data files and service agreements to allow forr customization based on individual requirements.

- Batch Ingestion Pipelines (```code/tenants/<tenant_id>/<tenant_id>_batchingestpipeline.py```): Each tenant will provide their own custom pipeline to handle the ingestion and processing of their data.




> 5. Implement and provide logging features for capturing successful/failed ingestion and transformation
tasks as well as metrics about ingestion time, data size, etc., for files which have been ingested and
transformed into mysimbdp. Logging information must be stored in separate files, databases or a
monitoring system for analytics of ingestion/transformation. Explain how mysimbdp could use such
logging information. Show and explain simple statistical data extracted from the logs for individual
tenants and for the whole platform with your tests in Point 4.1. (1 point)


The logger is implemented for each tenant and log files are stored under the ```tenants/<teanantid>/logging/batch``` to ensure organized and isolated logging information between tenants and different mode. The log files include pipeline status, capturing the success or failure of data ingestion and transformation tasks.  This allow tenant to monitor system health and catch any errors or bottlenecks in the data pipeline. For example, the logs capture failed ingestion tasks, errors in data transformation, and issues with data storage. If a tenant's data fails to ingest or process, logs can pinpoint where the issue occurred (e.g., file not found, data validation failure, transformation errors). This status information is stored locally within the tenant’s log folder for easy access and tracking in case of success or failure. 


For performance metrics, key metrics such as ingestion time, data size, and transformation duration are logged to monitor and assess system performance. These metrics are stored in Elasticsearch, where they are indexed under <tenant_id>_metrics for efficient querying and analysis. Using Elasticsearch enables the platform to easily calculate and visualize these metrics for better performance optimization and insights. Furthermore, information about performance metrics also displayed in the pipeline status log files above. These logs can be used to calculate per-tenant metrics, such as the number of successful ingestions, the average time taken to process data, and the size of data ingested per tenant. This can help track the performance of each tenant's data pipeline and highlight tenants who may be underperforming or experiencing issues.


Example of the metric for tenant1 is as follows:

![Metric Log](./pic/metric_log.png)







# Part 2 - Near real-time data ingestion and transformation 

1. Tenants will send original data via a messaging system for ingestion and transformation. The
messaging system mysimbdp-messagingsystem is provisioned by mysimbdp. Tenants will develop
message structures for data records. Tenants will develop ingestion and transformation pipelines,
streamingestpipeline, which read data from the messaging system, then transform and ingest the
data into mysimbdp-coredms. For near real-time ingestion, explain your design for the multi-tenancy
model in mysimbdp: which parts of the mysimbdp will be shared for all tenants, which parts will be
dedicated for individual tenants so that mysimbdp can add and remove tenants based on the principle
of pay-per-use. (1 point)


### Shared Components

-  Core Data Management System (```CoreDMS```) under directory ``` code/coredms```: 
The core DMS is structured as an Elasticsearch cluster consisting of three nodes. This cluster serves as the central repository for all ingested and transformed data across multiple tenants. 


- Messaging System (```code/streaming/kafka-docker-compose.yml```) consists of Kafka cluster
that is shared by all tenants. In mysimbdp, Kafka cluster is deployed and shared by all tenants. This unified messaging infrastructure  multiple tenants to send their data to the same system and leverage the message queues for efficient communication. Each tenant can use separate Kafka topics to keep their data isolated (e.g., tenant1_topic, tenant2_topic, etc.). Topics are logically separated within the shared Kafka cluster.
Partitioning: Kafka partitions a

 - Streaming Ingestion Manager (```StreamingIngestManager```) under directory ```code/streaming/streamingingestmanager.py```: The streaming ingestion manager is responsible for controlling the execution of multiple streamingestpipeline instances, starting and stopping them as required. It acts as an orchestrator for managing the lifecycle of pipelines.


 - Monitoring and alerting system (```mysimbdp-monitoring```): The monitoring and alerting system is implemented using Prometheus, an open-source monitoring system that is well-suited for tracking and evaluating the health, performance, and data quality of systems in real-time. For the streaming ingestion pipeline, Prometheus collects and analyzes logs and metrics from various ingestion processes, including Kafka producer/consumer interactions, message delivery times, throughput, ingestion latency, and message sizes. 

### Dedicated components
- Tenant-speicific congfiguration file (``code/tenants/<tenant_id>/<tenant_id>_config.yaml``): Each tenant has dedicated configuration files specifying constraints on data files and service agreements to allow forr customization based on individual requirements.

- Stream Ingestion Pipelines (```code/tenants/<tenant_id>/<tenant_id>_streamingestpipeline.py```): This represents a unit of work that processes data streams for a particular tenant. The pipeline consists of different steps that help in consuming data from producer, processing, and sending data to core DMS.



> 2. Design and implement a component mysimbdp-streamingestmanager, which can start and stop
streamingestpipeline instances on-demand. mysimbdp imposes the model that
streamingestpipeline has to follow so that mysimbdp-streamingestmanager can invoke
streamingestpipeline as a blackbox. Explain the model w.r.t. steps and what the tenant has to do in
order to write streamingestpipeline. (1 point)



The ```StreamIngestManager``` is responsible for invoking tenant-specific batching ingestion and transformation pipelines.

#### 1. Tenant Discovery and Pipeline Identification

To determine which str to execute for each tenant, the ```StreamIngestManager``` performs the following steps:

- Tenant Configuration: The platform maintains a central configuration file (```code/tenants/tenants.yaml```) that lists all tenants, including their unique tenant_id. Through the unique_id, more information such as their directory path can be easily retrieved.

- Pipeline Discovery: The ```StreamIngestManager``` scans the designated directory for each tenant and searching for a pipeline script with the naming convention ```<tenant_id>_streamingingestpipeline.py```. This dynamic discovery allows for easy addition or removal of tenant pipelines without modifying the core system.

- Modular model imposed: Each ```<tenant_id>_streamingingestpipeline.py``` script must conform to a standard model set by ```mysimbdp```, with  the inclusion of a required function ```main()```. By default, the ```StreamIngestManager``` invokes the ```main()``` function in each tenant-specific pipeline, which handles all the steps in tenant's pipeline.  Despite adhering to this common model, each pipeline remains a black box for the ```StreamIngestManager```, which enables customizations tailored to individual tenants.


#### 2. Execution of Pipeline
Once the ```StreamIngestManager``` has discovered the appropriate pipeline for each tenant, it schedules and invokes these pipelines based on various criteria, including tenant-specific configurations.

- Pipeline Execution: The ```StreamIngestManager``` will not require knowledge of the specific details of each tenant’s pipeline but instead, it will dynamically load and execute the pipeline script using Python's ```importlib``` module. This enables the platform to work with new tenants and pipelines without requiring code changes.


#### 3. Isolation and Flexibility
- Tenant Isolation: Each tenant's pipeline runs in isolation and concurrently to ensure that the execution of one tenant’s pipeline does not interfere with the others. This isolation is particularly important when dealing with multi-tenancy scenarios, where data security and privacy are critical.

- Flexibility for New Tenants: The ```StreamIngestManager``` is designed to be scalable. New tenants can be added without modifying the core system by simply adding their correspondiung configuration files and scripts following the prescribed model. 



3. Develop test ingestion programs (streamingestpipeline), which must include one type of data
wrangling (transforming the received message to a new structure) and the message structure for
data reports must be dened by a data schema. Show the performance of ingestion and
transformation tests, including failures and exceptions, for at least 2 dierent tenants in your test environment. Explain the data used for testing. (1 point)


The data used for testing is in the folders ```data```. The performance metrics can be seen through Prometheus in the question 4. Logging is also configured for each tenant under ```tenants/<tenant_id>/logging/stream```, where successful or failure can be captured. 

> 4. streamingestpipeline decides to report its processing performance, including average ingestion time,total ingestion data size, and number of messages to mysimbdp-streamingestmonitor. Design the
report format and explain possible components, ows and the mechanism for reporting. (1 point)



### Prometheus Metrics

#### Consumer Metrics
| Metric Name | Description | Type |
|-------------|------------|------|
| `kafka_messages_consumed_total` | Total number of messages consumed from Kafka | Counter |
| `kafka_consumption_errors_total` | Total number of errors encountered while consuming messages | Counter |
| `kafka_partition_eof_total` | Total number of end-of-partition events | Counter |
| `kafka_consumption_latency_seconds` | Time taken to consume and process a message | Summary |

#### Producer Metrics
| Metric Name | Description | Type |
|-------------|------------|------|
| `kafka_messages_sent_total` | Total number of messages produced and sent to Kafka | Counter |
| `kafka_messages_delivered_successful` | Number of successfully delivered messages | Counter |
| `kafka_message_delivery_latency_seconds` | Latency of Kafka message delivery | Summary |
| `kafka_total_ingested_data_size_bytes` |Total size of ingested data in bytes | Counter |
| `kafka_average_ingestion_time_seconds` |Average ingestion time per message | Gauge |

---


#### Flow and Mechanism of Reporting

1. Data Ingestion and Metric Collection: 
The mysimbdp-streamingestpipeline reads incoming CSV data using Polars and streams it to Kafka.
During ingestion, the pipeline records message count, delivery latency, data size, and average processing time.
These metrics are exposed via an HTTP endpoint on port 8000, accessible to Prometheus.
2. Prometheus Scraping: 
Prometheus is configured to scrape data from http://localhost:8000/metrics at regular intervals (every 10 seconds).
The collected metrics are stored in Prometheus (available at http://localhost:9090) for analysis and visualization.


> 5. Implement a feature in mysimbdp-streamingestmonitor to receive the report from
streamingestpipeline. Based on the report from streamingestpipeline, when the performance is
below a threshold, e.g., average ingestion time is too low, mysimbdp-streamingestmonitor decides
to inform mysimbdp-streamingestmanager about the situation. Implement a feature in mysimbdpstreamingestmanager to receive information informed by mysimbdp-streamingestmonitor. (1
point)


The alerts are configure for Prometheus. All rules can be in ```alerts.rules.yaml``` and is attached to the ```promtheus.yaml```. This will trigger alert whenever failure or low performance. Due to time constraints, the integration between mysimbdp-streamingestmonitor and mysimbdp-streamingestmanager for automated feedback has not yet been implemented. However, the alerting system in Prometheus has been successfully configured, and it will trigger alerts whenever conditions deviate from expected thresholds, particularly regarding ingestion performance.





# Part 3 - Integration and Extension
> 1. Produce an integrated architecture, with a figure, for the logging and monitoring of both batch and
near real-time ingestion and transformation features (Part 1, Point 5 and Part 2, Points 4-5). Explain
how a platform provider could know the amount of data ingested and existing errors/performance for
individual tenants which run both batch and near real-time ingestion/transformation pipelines. (1
point)

> 2. In the stream ingestion pipeline, assume that a tenant has to ingest and transform the same data but to different data sinks, e.g., mybdp-coredms for storage and a new mybdp-extradatasink component. What features/solutions can you provide and recommend to your tenant? (1 point)

In this context, each sink represents a destination where the data is either stored or processed. The key features and solutions that can be provided to address the tenant's requirements for a multi-sink setup include:


####  Parallelism for Multi-Sink Delivey

This includes parallel processing in the ingestion pipeline to send data to mybdp-coredms and mybdp-extradatasink concurrently to reduce the time taken to deliver data to multiple sinks to ensure faster processing and improved throughput. This can be executed by utilizing multi-threading or asynchronous task execution to ensure that while one transformation or data routing task is being processed for mybdp-coredms, another is being executed for mybdp-extradatasink.


#### Dynamic Data Routing
This involves introducing dynamic routing and conditional logic to determine the processing flow of each data record before sending it to the appropriate sinks. By implementing dynamic routing, data can be efficiently distributed to the correct sink, as some data may need to be sent to both sinks, while other data may be directed to only one, depending on its content or type. This decision is made based on predefined conditions or metadata.


### 

> 3. Assume that the tenant wants to protect the data during the ingestion and transformation by using
some encryption mechanisms to encrypt data in les. Thus, batchingestpipeline has to deal with
encrypted data. Which features/solutions do you recommend to the tenants, and which services might
you support for this goal? (1 point)

Below are the recommended features and solutions that can be integrated into the Streaming Ingest Pipeline to protect the data while ensuring smooth ingestion and transformation processes for encrypted data:

#### Encryption at Rest and in Transit

- Encryption at Rest (when storing data): Data will be encrypted using algorithms like *AES-256* before storing into data sinks or databases. This ensures that unauthorized users will not be able to read the content without the proper keys.

- Encryption in Transit (when the data is being transmitted across systems): As the data moves through the pipeline (e.g., between the producer and the Kafka broker, or between Kafka and the downstream data sinks), encryption in transit will be provided using protocols like TLS/SSL. This prevents from potential interception or tampering.


 #### Encryption Workflow 
This involves extends the Streaming Ingest Pipeline to include an encryption and decryption module. 
- Data Ingestion: Before ingesting data into the platform, data at rest is encrypted using AES-256 encryption.
- Transformation: During transformation, the encrypted data will be decrypted for processing using the decryption logic.
- Re-encryption (optional): After transformation, if the data needs to be securely stored in another sink or transferred, the transformed data can be reencrypted before being sent to its final destination.

#### Secure Key Management
Proper management of keys used in the encryption keys used in the decryption and encryption processes is crucial in preventing unauthorized access and misuse of keys. For this, services such as AWS KMS, Azure Key Vault, or HashiCorp Vault can be used to securely store and manage encryption keys, providing features like key rotation, access control, and auditing. By integrating a Key Management Service (KMS) into the streaming ingestion pipeline, you can ensure that only authorized services, such as the pipeline itself, can access these keys. 

> 4. In the case of near real-time ingestion, assume that we want to (i) detect the quality of data to ingest/transform only data with a predened quality of data and (ii) store the data quality detected
into the platform. Given your implementation in Part 2, how would you suggest a design/change for
achieving this goal? (1 point)


- Impolement the dynamic quality rules engine to allow tenants to define their own validation rules using a configuration file or through an API without needing to modify the underlying codebase of the platform. Parse the rules defined by the tenant in configuration files or APIs. The engine parses the rules specified by the tenant in these configuration files or APIs and applies them to validate incoming data at different stages of the pipeline. 

- Incorporate the data quality validation into the existing Kafka producer before sending to Kafka and into the Kafka consumer after consuming from Kafka. 
The producer can validate the data before sending it to Kafka. If the data does not meet the quality requirements, it can either be discarded or sent to a separate "rejected" topic to track low-quality data that was not ingested. On the other hand, the consumer can perform data quality checks when consuming data. If the data is valid, it can proceed to the transformation or storage pipeline. If not, it can trigger alerts (e.g send the alerts into Teams wrebhood) or send the data to a dead-letter queue (DLQ) for further investigation.

- Track data quality metrics (data quality failures, alert threshold, or rejected records) using Prometheus or similar tools for monitoring and set up alerts (via email, Slack, Teams, etc.) to notify stakeholders for significant quality issues. Dashboards or reports also can be integrated into this for visibility into data quality.

- Implement data lineage to trace the journey of data from its source to its final destination. This helps in tracking data quality issues and understanding how and where data quality problems are introduced in the pipeline. This will make troubleshooting and remediation much easier.



> 5. Assume a tenant has multiple batchingestpipeline. Each is suitable for a type of data, has dierent
workloads and service agreements, such as complex transformation or feature engineering (e.g., lead
to dierent CPU needs, memory consumption, and execution time). How would you extend your
design and implementation in Part 1 (only explain the concept/design) to support this requirement? (1
point)


- Pipeline Configuration Profiles:  In this extended design, each ```batchingestpipeline``` is treated as a distinct processing module with its own configuration parameters (e.g., transformation complexity, feature engineering needs) and resource requirements (such as CPU, memory, and execution time). When creating a pipeline instance,this profile is loaded to configure the pipeline accordingly. Each tenant's pipelines operate independently to prevent data leakage and maintain security.

- Orchestation and Dynamic Resource Allocation:  A central pipeline orchestrator or scheduler (```batchingestmanager```) manages these pipelines by dynamically allocating resources based on their profiles and service-level agreements (SLAs). This orchestration layer can leverage containerization and orchestration platforms (e.g., Kubernetes) to isolate and scale individual pipeline instances (each pipeline can be served as a Kubernetes pod). Resources will be dynamically adjusted based on real-time metrics (CPU, memory) using auto-scaling (for example Horizontal Pod Autoscaler (HPA) in Kubernetes).

- Monitoring and Logging: Each pipeline will expose relevant performance metrics (e.g., processing time, resourceonsumption) separeatly via Prometheus to track whether the pipelines are meeting their SLA goals. Logs from each pipeline will be aggregated into a centralized logging service for further detailed debugging and performance analysis.
 
- Batch execution control: Implement a Job Scheduler and Queue System (e.g., Airflow) to manage execution flow, priority, and SLA compliance.

