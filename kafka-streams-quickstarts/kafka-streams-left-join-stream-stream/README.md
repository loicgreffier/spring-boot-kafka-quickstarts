# Kafka Streams Left Join Stream Stream

This module demonstrates the following:

- The use of the Kafka Streams DSL, including `leftJoin()` between KStream and KStream, `selectKey()`, `peek()`.
- The use of sliding time windows.
- Unit testing using Topology Test Driver.

In this module, records of type `<String, KafkaUser>` are streamed from two topics named `USER_TOPIC`
and `USER_TOPIC_TWO`.
The following tasks are performed:

1. Join the records on the last name within a 5-minute join window and a 1-minute grace period for delayed records.
2. Build a new `KafkaJoinUsers` object that holds both users. If no user is matched for a record, a value holding
   the left user is still emitted as a result of the `leftJoin()` operation.
3. Write the resulting `KafkaJoinUsers` objects to a new topic named `USER_LEFT_JOIN_STREAM_STREAM_TOPIC`.

![topology.png](topology.png)

## Prerequisites

To compile and run this demo, you will need the following:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaUser>` to topics named `USER_TOPIC` and `USER_TOPIC_TWO`. You can use the [producer user](../specific-producers/kafka-streams-producer-user) to do this.
- Start the Kafka Streams.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Schema registry
- 1 Control Center
- 1 producer User
- 1 Kafka Streams Left Join Stream Stream
