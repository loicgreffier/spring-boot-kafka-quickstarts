# Kafka Streams Join Stream Table

This module streams records of type `<String, KafkaUser>` from the `USER_TOPIC` and joins them by nationality with records of type `<String, KafkaCountry>` from the `COUNTRY_TOPIC`.
It demonstrates the following:

- How to use the Kafka Streams DSL to join a `KStream` with a `KTable` using `join()`, `selectKey()` and `peek()`.
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
- Produce records of type `<String, KafkaCountry>` to the `COUNTRY_TOPIC`. You can use the [Producer Country](../specific-producers/kafka-streams-producer-country) for this.
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
- Producer Country
- Producer User
- Kafka Streams Join Stream Table
