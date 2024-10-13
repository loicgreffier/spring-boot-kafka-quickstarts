# Kafka Streams Store Timestamped Key-Value

This module demonstrates the following:

- The two strategies for creating timestamped Key-Value stores and attaching them to the topology.
- The usage of the Processor API, including `process()` and `addStateStore()`.
- Unit testing using Topology Test Driver.

In this module, records of type `<String, KafkaPerson>` are streamed from a topic named `PERSON_TOPIC`.
The following tasks are performed:

1. Create a first stream that pushes records to a timestamped Key-Value store named `PERSON_TIMESTAMPED_KEY_VALUE_STORE`.
2. Create a second stream that pushes records to another timestamped Key-Value store named `PERSON_TIMESTAMPED_KEY_VALUE_SUPPLIER_STORE`.

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
- 1 Kafka Streams Store Timestamped Key-Value
