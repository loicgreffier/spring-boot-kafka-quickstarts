# Kafka Streams Branch

This module demonstrates the following:

- The use of the Kafka Streams DSL, including `branch()` and `peek()`.
- Unit testing using Topology Test Driver.

In this module, records of type `<String, KafkaUser>` are streamed from a topic named `USER_TOPIC`.
The following tasks are performed:

1. Split the records into different topics based on the last name of each `KafkaUser` record:

- Users with a last name starting with "S" are sent to a topic named `USER_BRANCH_A_TOPIC`. Both the first name and
  last name are converted to uppercase.
- Users with a last name starting with "F" are sent to a topic named `USER_BRANCH_B_TOPIC`.
- Other users (with last names not starting with "A" or "B") are sent to a topic named `USER_BRANCH_DEFAULT_TOPIC`.

![topology.png](topology.png)

## Prerequisites

To compile and run this demo, you’ll need:

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
- 1 Schema Registry
- 1 Control Center
- 1 producer User
- 1 Kafka Streams Branch
