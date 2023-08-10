# Consumer Transactional

This module demonstrates the following:

- The usage of the Kafka Clients consumer DSL.
- The usage of isolation level.

This module consumes records of type `<String, String>` from topics named `FIRST_STRING_TOPIC` and `SECOND_STRING_TOPIC`. It utilizes an isolation level of `read_committed` to ensure that only committed records are consumed, filtering out any uncommitted or transactionally aborted records.

## Requirements

To compile and run this demo, you will need the following:

- Java 17
- Maven
- Docker

## Running the Application

To run the application manually, please follow the steps below:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, String>` to topics named `FIRST_STRING_TOPIC` and `SECOND_STRING_TOPIC`. You can use the [producer transaction](../../kafka-producer-quickstarts/kafka-producer-transaction) to do this.
- Start the consumer.

To run the application in Docker, please use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Zookeeper
- 1 Kafka broker
- 1 Control Center
- 1 producer transaction
- 1 consumer transaction
