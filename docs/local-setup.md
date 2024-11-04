# Setup for running the demo locally

For testing and development, you can run the demo partly locally:

* Run the Flink applications, [Vehicle event generator](./vehicle-event-generator), [Pre-processor](./pre-processor),
  and [Raw event writer](./raw-event-writer) directly in IntelliJ. You do not need to install Apache Flink locally.
* Run Kafka locally, in a container. This simplifies the development setup not requiring access to MSK from
  your development machine
* You can use Amazon Managed Prometheus and Amazon Managed Grafana when running the Flink application locally, without
  setting up any special connectivity, as long as on your machine you are using AWS credentials with
  [Remote-Write permissions](#iam-permissions-to-use-amp-remote-write) to the AMP workspace.

## Prerequisites for running the demo locally

When running the demo locally you still use the AMP workspace and Grafana dashboard on AWS.
See the details in [manual step-by-step](manual-step-by-step.md) instructions to set up these components.

* IDE AWS plugin
* Docker or equivalent
* Valid AWS credentials or authenticated session, from your development machine, with
  [Remote-Write permissions](#iam-permissions-to-use-amp-remote-write) to the AMP workspace.


## Run Kafka in container

You can use the [Docker compose stack](./kafka-docker) to run a small Kafka cluster (single broker) locally.

1. Ensure you have Docker (or equivalent) running on your machine
2. In a new console window, from the `./kafka-docker` folder, run `docker compose up`

To stop the stack press Ctrl-C.

> You can alternatively use the IntelliJ docker integration to run
> [`kafka-docker/docker-compose.yaml`](./kafka-docker/docker-compose.yaml).

This stack creates
* A Kafka cluster (single broker) with bootstrap server: `localhost:9092` (PLAINTEXT)
* A topic named `vehicle-events`
* [Kafka-UI](https://github.com/provectus/kafka-ui) is also available on http://localhost:8080, to inspect the content of the topic


## IAM permissions to use AMP Remote-Write

The AWS credentials you use on your machine should have Remote-Write permissions to the AMP workspace.
You can either:
* Ensure you have `aps:RemoteWrite` permissions to the AMP workspace ARN
* or, add the `AmazonPrometheusRemoteWriteAccess` policy to the user

> Note: if your user already has broader permissions, you do not need to add this.

## Run the Flink applications locally

The three Flink applications can run locally, in the IDE, without any code changes.
You do not need to run a local Flink cluster or installing Flink on your development machine.

### Set up the local runtime configuration

The runtime configuration the applications use when running locally are in JSON files named
`flink-application-properties-dev.json` in the `resources` folder of each application.

You should modify these files to match your actual configuration:
* [Vehicle event generator config](vehicle-event-generator/src/main/resources/flink-application-properties-dev.json):
  no changes required
* [Pre-processor config](pre-processor/src/main/resources/flink-application-properties-dev.json):
  change `PrometheusSink`, `endpoint.url` to match your AMP Remote Write URL
* [Raw event writer config](raw-event-writer/src/main/resources/flink-application-properties-dev.json):
  change `PrometheusSink`, `endpoint.url` to match your AMP Remote Write URL

### Set up AWS profile for the applications

Ensure you have a valid AWS authentication for AWS CLI on your machine. The simplest way is creating a *profile* for the CLI.
See [Configuring settings for the AWS CLI](https://docs.aws.amazon.com/cli/v1/userguide/cli-chap-configure.html) for details.

Using the [AWS Toolkit for IntelliJ IDEA](https://aws.amazon.com/intellij/), select the profile name and the region
you are using for the demo.

### Set up the Flink job Run configurations

Before running the Flink jobs from IntelliJ, you need to set up the Run configurations for the three Java classes that
contain the `main()` of the three jobs:
1. [`vehicle-event-generator` : `VehicleEventGeneratorJob.java`](../vehicle-event-generator/src/main/java/com/amazonaws/examples/flink/VehicleEventGeneratorJob.java)
2. [`pre-processor` : `PreProcessorJob.jar`](../pre-processor/src/main/java/com/amazonaws/examples/flink/PreProcessorJob.java)
3. [`raw-event-writer` : `RawEventWriterJob.jar`](../raw-event-writer/src/main/java/com/amazonaws/examples/flink/RawEventWriterJob.java)

All three classes must have a similar Run configuration:
* *Run options*: *Add dependencies with "provided" scope to classpath*
* *AWS Connection*: *Use the currently selected profile credentials/region*

### Run the Flink jobs from IntelliJ

1. Ensure the Kafka Docker compose stack is running.
2. Run `vehicle-event-generator` > `VehicleEventGeneratorJob.java` from IntelliJ as a normal Java application
3. To verify the generator is correctly publishing events to the local Kafka
   * Open the Kafka UI console at [http://localhost:8080](http://localhost:8080)
   * Select *Topics* > `vehicle-events`> *Messages* to view the events
4. Run `pre-processor` > `PreProcessorJob.jar`

After a few seconds you should see datapoints appearing on the Grafana dashboard.

You can also optionally run `raw-event-writer` > `RawEventWriterJob.jar` in the same way.
