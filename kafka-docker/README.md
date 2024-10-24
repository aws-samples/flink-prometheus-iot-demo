# Local Kafka (Docker Compose)

This folder contains a Docker compose file to run a very simple Kafka cluster locally.
This may simplify local development and debugging, removing the need of a network access to the real Kafka cluster.

The Docker Compose file spins up:
* A single-node Kafka cluster, PLAINTEXT only, in KRAFT mode (no ZK) on `localhost:9092`
* [Kafka-UI](https://github.com/provectus/kafka-ui) on http://localhost:8080

On start, it creates a topic named `vehicle-events` with 3 partitions, if it does not exist already.
