# Kafka Streams Join Stream Table

This module demonstrates the following:

- The usage of the Kafka Streams DSL, including `join()` between KStream and KTable, `selectKey()` and `peek()`.
- Unit testing using the Topology Test Driver.

In this module, records of type `<String, KafkaPerson>` are streamed from a topic named `PERSON_TOPIC`, and records of type `<String, KafkaCountry>` are streamed from a topic named `COUNTRY_TOPIC`.
The following tasks are performed:

1. Join the two streams on the country code and create a new object `KafkaJoinPersonCountry` that contains the person and country information.
2. Write the resulting `KafkaJoinPersonCountry` objects to a new topic named `PERSON_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC`.

![topology.png](topology.png)

## Requirements

To compile and run this demo, you will need the following:

- Java 17
- Maven
- Docker

## Running the Application

To run the application manually, please follow the steps below:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaCountry>` to a topic named `COUNTRY_TOPIC`. You can use the [producer country](../specific-producers/kafka-streams-producer-country) to do this.
- Produce records of type `<String, KafkaPerson>` to a topic named `PERSON_TOPIC`. You can use the [producer person](../specific-producers/kafka-streams-producer-person) to do this.
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
- 1 producer country
- 1 producer person
- 1 Kafka Streams join stream table
