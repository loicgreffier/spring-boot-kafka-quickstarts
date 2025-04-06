# Consumer Circuit Breaker

This module consumes records of type `<String, KafkaUser>` from a topic named `USER_TOPIC`, handling deserialization exceptions.
It demonstrates the following:
- Use of the Kafka Clients consumer API
- How to handle poison pills in a Kafka consumer
- Use of Apache Avro
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
- Ensure a deserialization exception will occur by either:
    - Deleting the Avro schema from the schema registry
    - Producing a String message in the middle of Avro messages
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
- 1 Circuit Breaker Consumer
