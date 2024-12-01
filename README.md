# Kafka Data Pipeline

## Objective:
Design and implement a Kafka consumer to consume data
from a Kafka topic and perform some basic processing on the data and identify some insights, if possible.

Configure another Kafka Topic and store the processed data in the new topic

Ensure that the pipeline can handle the streaming data continuously and efficiently,
handling any error messages or missing fields

## Design
This is a simple pipeline for consuming the messages from `my-python-producer`, transforming and filtering some of the data, and then sending it to a new topin called `processed_data`.  

There is some error catching in place to determine if the data is incomplete. In doing this we can see that the data field `device_type` is what we will typically see missing. The pipeline will not fail when we hit errors, but will instead not process the data.  

## Running Locally

Spin up docker container: `docker compose up -d`

Ensure you have the python client installed: `pip install kafka-python`

Run `python3 kafka_pipeline.py`

You can then monitor the output to see the messages being consumed and sent, as well as the errors.

You can consume the processed messages using this code: 
```
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'processed_data',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',  # Start from the earliest message
    enable_auto_commit=True,
    group_id='processed_data_group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Deserialize JSON messages
)

# Consume messages
for message in consumer:
    print(f"Consumed message: {message.value}")
```

## Additional Questions
1. How would you deploy this application in production?
   - Create a CI/CD pipeline to automate testing and deployments. Ensure manual steps between lower envs and production using tags, release branches, or manual approval through UI.
   - Setup Grafana or similar tool for real time monitoring and logging.
   - Ensure the Kafka cluster has at least 3 brokers and the topics have a replication factor of at least 3.  
   - Use managed Kafka services to manage infrastructure, scaling, and security.
   - Use Terraform to provision the necessary cloud resources.
   - Optimize partitioning of the topics to distribute load and match the number of consumers.
2. What other components would you want to add to make this production ready?
  One would be to create a new topic to consume all the errored messages that did not get processed so we can analyize this and correct data issues if necessary. Another would be to store the insights data for further analysis.  This would allow monitoring and visibility into the pipeline and the data that is flowing through it. You could also create alerting based off the insights.  I would also want to refactor the code to ensure better readability and efficiency.
3. How can this application scale with a growing dataset?
   - Use Kafka managed services for autoscaling consumers and producers based on metrics.
   - Design the topics to have sufficient partitions and ensure there are enough consumers for parallelism.