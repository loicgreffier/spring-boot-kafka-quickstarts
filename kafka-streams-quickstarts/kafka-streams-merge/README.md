# Kafka Streams Merge

This module demonstrates the following:

- The usage of the Kafka Streams DSL, including `merge()` and `peek()`.
- Unit testing using Topology Test Driver.

In this module, records of type `<String, KafkaPerson>` are streamed from two topics named `PERSON_TOPIC`
and `PERSON_TOPIC_TWO`.
The following tasks are performed:

1. Merge the two streams into a single stream.
2. Write the merged stream to a new topic named `PERSON_MERGE_TOPIC`.

![topology.png](topology.png)

## Requirements

To compile and run this demo, you will need the following:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually, please follow the steps below:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaPerson>` to a topic named `PERSON_TOPIC`. You can use the [producer person](../specific-producers/kafka-streams-producer-person) to do this.
- Start the Kafka Streams.

To run the application in Docker, please use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka broker KRaft
- 1 Schema registry
- 1 Control Center
- 1 producer Person
- 1 Kafka Streams Merge