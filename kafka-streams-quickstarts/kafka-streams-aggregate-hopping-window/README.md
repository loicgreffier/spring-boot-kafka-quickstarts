# Kafka Streams Aggregate Hopping Window

This module streams records of type `<String, KafkaUser>` from the `USER_TOPIC` and aggregates users by last name using a 5-minute time window with a 2-minute hop, and a 1-minute grace period for
delayed records.
It demonstrates the following:

- How to use the Kafka Streams DSL, including `aggregate()`, `windowedBy().advanceBy()`, `groupByKey()`, `selectKey()`, `toStream()` and `peek()`.
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

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Schema Registry
- Control Center
- Producer User
- Kafka Streams Aggregate Hopping Window
