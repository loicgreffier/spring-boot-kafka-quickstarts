# Consumer Avro Specific

This module consumes records of type `<String, KafkaUser>` from a topic named `USER_TOPIC`.
It demonstrates the following:
- Use of the Kafka Clients consumer API
- Use of Apache Avro and specific records
- Unit testing with a mock consumer

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaUser>` to the `USER_TOPIC`. You can use the [producer Avro Specific](../../kafka-producer-quickstarts/kafka-producer-avro-specific) for this.
- Start the consumer.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command starts the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Schema Registry
- 1 Control Center
- 1 Producer Avro Specific
- 1 Consumer Avro Specific
