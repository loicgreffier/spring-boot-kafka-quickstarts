# Kafka Streams Average

This module demonstrates the following:

- The use of the Kafka Streams DSL, including `aggregate()`, `groupBy()`, `mapValues()`, `toStream()` and `peek()`.
- Unit testing using Topology Test Driver.

In this module, records of type `<String, KafkaUser>` are streamed from a topic named `USER_TOPIC`.
The following tasks are performed:

1. Group the stream by nationality using `groupBy()` operation.
2. Apply an aggregator that calculates the sum of ages for each `KafkaUser` record with the same nationality. The
   aggregator produces a `KafkaAverageAge` object that holds the sum and count of ages.
3. Compute the average age by nationality. The result is written to a new topic named `USER_AVERAGE_TOPIC`.

![topology.png](topology.png)

## Prerequisites

To compile and run this demo, you will need the following:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaUser>` to a topic named `USER_TOPIC`. You can use the [producer user](../specific-producers/kafka-streams-producer-user) to do this.
- Start the Kafka Streams application.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Schema registry
- 1 Control Center
- 1 producer User
- 1 Kafka Streams Average
