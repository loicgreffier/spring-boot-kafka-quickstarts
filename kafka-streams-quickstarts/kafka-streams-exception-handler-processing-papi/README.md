# Kafka Streams Processing Exception Handler PAPI

This module streams records of type `<String, KafkaUser>` from the `USER_TOPIC` and handles processing exceptions in the Processor API.
It demonstrates the following:

- How to use the Kafka Streams configuration `processing.exception.handler` to handle processing exceptions in the Processor API, as introduced by [KIP-1033](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1033%3A+Add+Kafka+Streams+exception+handler+for+exceptions+occurring+during+processing).
- How to implement a custom processing exception handler.
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

Alternatively, to run the application with Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka Broker (KRaft mode)
- 1 Schema Registry
- 1 Control Center
- 1 Producer User
- 1 Kafka Streams Exception Handler Processing
