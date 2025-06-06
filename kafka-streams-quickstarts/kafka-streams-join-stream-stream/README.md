# Kafka Streams Join Stream Stream

This module streams records of type `<String, KafkaUser>` from two topics: `USER_TOPIC` and `USER_TOPIC_TWO`,
and joins them by last name within a 5-minute window, allowing a 1-minute grace period for delayed records.
It demonstrates the following:

- How to use the Kafka Streams DSL to join two `KStream` using `join()`, `selectKey()` and `peek()`.
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
- Produce records of type `<String, KafkaUser>` to the `USER_TOPIC` and `USER_TOPIC_TWO`. You can use the [Producer User](../specific-producers/kafka-streams-producer-user) for this.
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
- 1 Kafka Streams Join Stream Stream
