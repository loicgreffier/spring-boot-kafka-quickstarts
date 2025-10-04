# Kafka Streams Cogroup

This module streams records of type `<String, KafkaUser>` from two topics: `USER_TOPIC` and `USER_TOPIC_TWO`.
It groups all streams and aggregates users by last name.
It demonstrates the following:

- How to use the Kafka Streams DSL, including `cogroup()`, `groupBy()`, `aggregate()`, `toStream()` and `peek()`.
- Unit testing using the Topology Test Driver.

![topology.png](topology.png)

## Prerequisites

To compile and run this demo, you’ll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaUser>` to topics named `USER_TOPIC` and `USER_TOPIC_TWO`. You can use the [Producer User](../specific-producers/kafka-streams-producer-user) for this.
- Start the Kafka Streams application.

Alternatively, to run the application with Docker, use the following command:

```console
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Schema Registry
- Control Center
- Producer User
- Kafka Streams Cogroup
