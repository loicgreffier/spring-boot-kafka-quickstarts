# Consumer Exactly Once Processing

This module consumes records of type `<String, KafkaUser>` from the `USER_TOPIC`, maps the first and last names to uppercase, and sends them to the `EXACTLY_ONCE_PROCESSING_TOPIC` withing a transaction.
It demonstrates the following:

- How to use the Kafka Clients consumer and producer APIs.
- How to use Kafka transactions to achieve exactly-once processing.
- Unit testing with a mock consumer.

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaUser>` to the `USER_TOPIC`. You can use the [Producer Avro Specific](../../kafka-producer-quickstarts/kafka-producer-avro-specific) for this.
- Start the consumer.

Alternatively, to run the application with Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka Broker (KRaft mode)
- 1 Schema Registry
- 1 Control Center
- 1 Producer Avro Specific
- 1 Consumer Exactly Once Processing
