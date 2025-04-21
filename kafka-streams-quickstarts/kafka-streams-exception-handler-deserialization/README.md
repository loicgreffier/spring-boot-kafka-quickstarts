# Kafka Streams Deserialization Exception Handler

This module streams records of type `<String, KafkaUser>` from the `USER_TOPIC` and handles deserialization exceptions.
It demonstrates the following:

- How to use the Kafka Streams configuration `default.deserialization.exception.handler` to handle deserialization exceptions.
- How to implement a custom deserialization exception handler.
- Unit testing using the Topology Test Driver.

![topology.png](topology.png)

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaUser>` to the `USER_TOPIC`. You can use the [Producer User](../specific-producers/kafka-streams-producer-user) for this.
- Start the Kafka Streams application.
- To activate the custom deserialization exception handler, produce a record with a value that cannot be deserialized to the `KafkaUser` type (e.g., a value that is not in Avro format) to the `USER_TOPIC`. You can use the Control Center for this.

Alternatively, to run the application with Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka Broker (KRaft mode)
- 1 Schema Registry
- 1 Control Center
- 1 Producer User
- 1 Kafka Streams Exception Handler Deserialization
