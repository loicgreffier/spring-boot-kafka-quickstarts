# Kafka Streams Print

This module demonstrates the usage of the Kafka Streams DSL `print()` and `peek()`.

In this module, records of type `<String, KafkaPerson>` are streamed from two topics named `PERSON_TOPIC`
and `PERSON_TOPIC_TWO`.
The following tasks are performed:

1. Stream records of type `<String, KafkaPerson>` from the topic `PERSON_TOPIC` and print them to the
   file `/tmp/kafka-streams-quickstarts/streams-print-output.log`.
2. Stream records of type `<String, KafkaPerson>` from the topic `PERSON_TOPIC_TWO` and print them to the system output.

![topology.png](topology.png)

## Requirements

To compile and run this demo, you will need the following:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually, please follow the steps below:

- Start
  a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp)
  in a Docker environment.
- Produce records of type `<String, KafkaPerson>` to topics named `PERSON_TOPIC` and `PERSON_TOPIC_TWO`. You can use
  the [producer person](../specific-producers/kafka-streams-producer-person) to do this.
- Start the Kafka Streams.

To run the application in Docker, please use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Zookeeper
- 1 Kafka broker
- 1 Schema registry
- 1 Control Center
- 1 producer person
- 1 Kafka Streams print