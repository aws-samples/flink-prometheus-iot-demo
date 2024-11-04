# Step-by-step instructions to set up the components of the demo, manually

This document describes how to set up manually all the components of the demo.

* An MSK cluster with unauthenticated access
* An [Amazon Managed Service for Prometheus (AMP)](https://aws.amazon.com/prometheus/) workspace
* An [Amazon Managed Grafana](https://aws.amazon.com/grafana/) workspace
* A VPC endpoint for AMP data-plane
* An S3 bucket, for the Flink application artifacts
* Three [Amazon Managed Service for Apache Flink](https://aws.amazon.com/managed-service-apache-flink/) applications:
  1. Vehicle event generator
  2. Pre-processor
  3. Raw event writer (optional)

### Networking

MSK cluster and all Managed Flink applications are deployed in the same VPC and in same 3x **private** Subnets, one per AZ.

MSK cluster and Managed Flink applications can also use the same Security Group:
* Inbound: All traffic from *self*
* Outbound: All traffic to `0.0.0.0/0`

### Set up an MSK cluster

Create an MSK cluster following [this documentation](https://docs.aws.amazon.com/msk/latest/developerguide/create-cluster.html)

* MSK cluster type: Provisioned
* Broker size: `kafka.m7g.large`
* Storage: 10 GB
* VPC and Subnets: select the 3 private Subnets

After you created the cluster, modify the configuration as follows:
* Security settings: enable unauthenticated access and plaintext "encryption"
* Cluster configuration: create a custom Cluster configuration adding or modifying the following parameters
    * `auto.create.topics.enable=true`
    * `num.partitions=64` (or less, see note below)

**Important**: the number of partitions must be equal or greater than the parallelism you will use for the Pre-processor
and Raw event writer Flink applications.

> Note: if you are using an existing cluster, you can manually create the topic named `vehicle_events`

Take note of the Bootstrap servers you can find in *View client configuration*.

Make sure you use the *Private endpoint (single-VPC) for Authentication type *Plaintext*.

### Set up AMP workspace

Create an Amazon Managed Prometheus workspace with the default configuration.

Take note of *Endpoint - remote write URL*.

### Set up VPC Endpoint for AMP data plane (workspaces)

Create a VPC Endpoint for AMP data plane (workspaces) (Service: `com.amazonaws.<region>.aps-workspaces`).

Connect it to the same VPC and Subnets you used for the MSK cluster.

### Build the Flink application and upload the JARs to the S3 buckets

Build this project running `mvn package` from the project root directory. This will create the JAR files of
the 3 application in the `target` subfolders of the application modules.

Upload these three JARs to the S3 bucket:
1. `./vehicle-event-generator/target/vehicle-event-generator_1.20.0.jar`
2. `./pre-processor/target/pre-processor_1.20.0.jar`
3. `./raw-event-writer/target/raw-event-writer_1.20.0.jar`

> The JARs to use are those **not** named `original-`.



### Create and run Vehicle Event Generator in Managed Service for Apache Flink

Create a Managed Service for Apache Flink application following [these instructions](https://docs.aws.amazon.com/managed-flink/latest/java/how-creating-apps.html).
Application name: `IoT-demo-event-generator`

Once the application is created, modify the configuration as follows:
* Application code location: select the S3 bucket and write the JAR name (`vehicle-event-generator_1.20.0.jar`)
* Scaling
    * Parallelism: 24
    * Parallelism per KPU: 1
* Networking: select VPC connectivity based on MSK cluster, select the MSK cluster

Add the following Runtime properties:

| Group ID    | Key                  | Value                      |
|-------------|----------------------|----------------------------|
| `DataGen`   | `events.per.sec`     | `50000`                    |
| `DataGen`   | `vehicles`           | `10000`                    |
| `KafkaSink` | `bootstrap.servers`  | the MSK plaintext endpoint |

For a description of all configuration parameters, see [Configuring Vehicle event generator](../README.md#configuring-vehicle-event-generator).

Leave all other default configurations.

Save the configuration and run the application.

### Run Pre-processor in Managed Service for Apache Flink

Create a Managed Service for Apache Flink application following [these instructions](https://docs.aws.amazon.com/managed-flink/latest/java/how-creating-apps.html).
Application name: `IoT-demo-pre-processor`

Once the application is created, modify the configuration as follows:
* Application code location: select the S3 bucket and write the JAR name (`pre-processor_1.20.0.jar`)
* Scaling
    * Parallelism: 64
    * Parallelism per KPU: 1
* Networking: select VPC connectivity based on MSK cluster, select the MSK cluster

Add the following Runtime properties:

| Group ID         | Key                 | Value                             |
|------------------|---------------------|-----------------------------------|
| `KafkaSource`    | `bootstrap.servers` | the MSK plaintext endpoint        |
| `PrometheusSink` | `endpoint.url`      | the AMP remote-write endpoint URL |

For a description of all configuration parameters, see [Configuring Pre-processor](../README.md#configuring-pre-processor).

Leave all other default configurations.

Save the configuration.

Modify the application IAM Role adding the policy `AmazonPrometheusRemoteWriteAccess`.

> Important: if you forget to modify the application IAM Role, the Pro-processor will not be able to write to AMP.

Run the application.


### (optional) Run Raw Event Writer in Managed Service for Apache Flink


Create a Managed Service for Apache Flink application following [these instructions](https://docs.aws.amazon.com/managed-flink/latest/java/how-creating-apps.html).
Application name: `IoT-demo-raw-event-writer`

Once the application is created, modify the configuration as follows:
* Application code location: select the S3 bucket and write the JAR name (`pre-processor_1.20.0.jar`)
* Scaling
    * Parallelism: 64
    * Parallelism per KPU: 1
* Networking: select VPC connectivity based on MSK cluster, select the MSK cluster

Add the following Runtime properties:

| Group ID         | Key                 | Value                             |
|------------------|---------------------|-----------------------------------|
| `KafkaSource`    | `bootstrap.servers` | the MSK plaintext endpoint        |
| `PrometheusSink` | `endpoint.url`      | the AMP remote-write endpoint URL |

For a description of all configuration parameters, see [Configuring Raw event writer](../README.md#configuring-raw-event-writer).

Leave all other default configurations.

Save the configuration.

Modify the application IAM Role adding the policy `AmazonPrometheusRemoteWriteAccess`.

> Important: if you forget to modify the application IAM Role, the Pro-processor will not be able to write to AMP.

Run the application


### Set up Managed Grafana workspace

Create a Managed Grafana workspace.

* Authentication access: AWS IAM Identity Center
* Permission type: service managed
* Network access control: Open access
* IAM permission access settings
    * Current account
    * Data sources: select *Amazon Managed Service for Prometheus*

> Note: for simplicity, the Grafana dashboard is publicly accessible

Take note of the *Grafana workspace URL*

#### Create a user in IAM Identity Center

Follow [these instructions](https://docs.aws.amazon.com/singlesignon/latest/userguide/addusers.html) to create a user
in IAM Identity Center.

* Only the basic details are requires for the user (username, email, fist and lastname, display name).
* Password: select *Generate a one-time password that you can share with this user.*

Leave all other default configuration.

Once you created the user, take note of the *AWS access portal URL*, *Username*, and *One-time password*

Access a first time, using the OTP, to set up the password.

**Important**: because the Grafana endpoint is publicly accessible, use a secure password.

#### Set up the Grafana user

You now need to add the IAM Identity user to the Grafana workspace.

1. In the AWS console page of the Grafana workspace, under *Authentication* > *AWS IAM Identity Center (successor to AWS SSO)*
   select *Assign new user or group*.
2. Select the identity you created assign it
3. Then, select again the identity and in the *Action* dropdown choose "Make admin"

### Create Grafana dashboard

1. Access the Grafana dashboard using the *Grafana workspace URL* of the workspace, and the credentials you just created.
2. Under *Apps* > *AWS Data Source*
    * Select *Amazon Managed Prometheus*
    * Select the region where you deployed the AMP workspace
    * Select the AMP workspace you created
    * Choose *Add new data source*
3. Under *Dashboards*
* *Create dashboard*
* *Add visualization*
* Select the AMP data source
* In  *Query* > *Metric* > *Metric explorer* : choose `vehicles_in_motion`
* Choose *Run query* - the metrics should appear in the chart, above
* Change *Panel Title* to "Vehicles in motion"
* Choose the *Apply* button, on the top right
4. Repeat *Add visualization* for the "Warnings" metric
5. Choose the *Save dashboard* icon in the top bar, to save the dashboard
