# CDK Deployment

Here you can find the instructions for deploying the following resources for the demo using CDK

- Amazon VPC
- Amazon MSK Cluster
- Amazon Managed Service for Prometheus
- Amazon Managed Service for Apache Flink Applications (Data Generator & Data Processor)
- CloudWatch Log groups and Log Streams
- AWS Lambda Functions for creating a Kafka Topic and starting the Flink applications.

## Pre-requisites

In order to deploy this solution you will need
- AWS Credentials and permissions for deploying CDK
- Docker running.

We use Docker in order to build the required Lambda Layer for being able to create the Kafka topic and for packaging the Apache Flink Applications using Maven

## Deployment

Assuming you have already git cloned this repository and are in the cdk folder

1. Run ```npm install```
2. Run ```cdk bootstrap```
3. Run ```cdk deploy```

When prompted to confirm deploying the resources type ```yes```

The solution should take around 30 minutes to deploy

### Setting Up Grafana

