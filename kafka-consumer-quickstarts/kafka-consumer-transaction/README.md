# Consumer Transactional

This module consumes records of type `<String, String>` from two topics: `FIRST_STRING_TOPIC` and `SECOND_STRING_TOPIC`.

It demonstrates the following:

- How to use the Kafka Clients consumer API.
- How to configure the consumer's `isolation.level` to `read_committed`, ensuring that only committed records are consumed while filtering out uncommitted or aborted transactional records.

## Prerequisites

To compile and run this demo, you'll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, String>` to the `FIRST_STRING_TOPIC` and `SECOND_STRING_TOPIC`. You can use the [Producer Transaction](../../kafka-producer-quickstarts/kafka-producer-transaction) for this.
- Start the consumer.

Alternatively, to run everything at once using Docker, run:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Control Center
- Producer Transaction
- Consumer Transaction