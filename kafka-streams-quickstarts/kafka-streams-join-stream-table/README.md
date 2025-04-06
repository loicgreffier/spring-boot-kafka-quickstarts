# Kafka Streams Join Stream Table

This module demonstrates the following:

- The use of the Kafka Streams DSL, including `join()` between KStream and KTable, `selectKey()` and `peek()`.
- Unit testing using Topology Test Driver.

In this module, records of type `<String, KafkaUser>` are streamed from a topic named `USER_TOPIC`, and records of
type `<String, KafkaCountry>` are streamed from a topic named `COUNTRY_TOPIC`.
The following tasks are performed:

1. Join the two streams on the country code and create a new object `KafkaJoinUserCountry` that contains the user
   and country information.
2. Write the resulting `KafkaJoinUserCountry` objects to a new topic
   named `USER_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC`.

![topology.png](topology.png)

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaCountry>` to a topic named `COUNTRY_TOPIC`. You can use the [producer country](../specific-producers/kafka-streams-producer-country) for this.
- Produce records of type `<String, KafkaUser>` to the `USER_TOPIC`. You can use the [producer user](../specific-producers/kafka-streams-producer-user) for this.
- Start the Kafka Streams application.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command starts the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Schema Registry
- 1 Control Center
- 1 Producer Country
- 1 Producer User
- 1 Kafka Streams Join Stream Table
