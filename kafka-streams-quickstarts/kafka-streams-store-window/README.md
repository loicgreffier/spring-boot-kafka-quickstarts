# Kafka Streams Store Window

This module streams records of type `<String, KafkaUser>` from the `USER_TOPIC` and stores them in two different window stores.
It demonstrates the following:

- How to use the Kafka Streams Processor API, including `process()` and `addStateStore()`.
- How to create window stores and attach them to the topology.
- Unit testing using the Topology Test Driver.

![topology.png](topology.png)

## Prerequisites

To compile and run this demo, you'll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaUser>` to the `USER_TOPIC`. You can use the [Producer User](../specific-producers/kafka-streams-producer-user) for this.
- Start the Kafka Streams application.

Alternatively, to run everything at once using Docker, run:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Schema Registry
- Control Center
- Producer User
- Kafka Streams Store Window
