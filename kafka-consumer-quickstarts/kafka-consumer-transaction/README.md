# Consumer Transactional

This module consumes records of type `<String, String>` from topics named `FIRST_STRING_TOPIC` and `SECOND_STRING_TOPIC` using the transaction isolation level read_committed.
It demonstrates the following:

- Use of the Kafka Clients consumer API
- Use of isolation levels

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, String>` to topics named `FIRST_STRING_TOPIC` and `SECOND_STRING_TOPIC`. You can use the [producer Transaction](../../kafka-producer-quickstarts/kafka-producer-transaction) for this.
- Start the consumer.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command starts the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Control Center
- 1 Producer Transaction
- 1 Consumer Transaction
