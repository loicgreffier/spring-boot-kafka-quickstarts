# Consumer Transactional

This module demonstrates the following:

- How to use the Kafka Clients consumer API.
- How to use the isolation level setting.

This module consumes records of type `<String, String>` from topics named `FIRST_STRING_TOPIC`
and `SECOND_STRING_TOPIC`. It uses an isolation level of `read_committed` to ensure that only committed records are
consumed, filtering out any uncommitted or transactionally aborted records.

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, String>` to topics named `FIRST_STRING_TOPIC` and `SECOND_STRING_TOPIC`. You can use the [producer transaction](../../kafka-producer-quickstarts/kafka-producer-transaction) to do this.
- Start the consumer.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Control Center
- 1 producer Transaction
- 1 consumer Transaction
