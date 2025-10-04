# Kafka Streams Schedule

This module streams records of type `<String, KafkaUser>` from the `USER_TOPIC` and uses the Processor API to count users by nationality. 
It then schedules the emission of these counts to the downstream processor based on wall clock time and stream time.
It demonstrates the following:

- How to use the Kafka Streams Processor API, including `process()` and `schedule()`.
- How to use the processor context and schedule tasks based on wall clock time and stream time.
- How to create a timestamped key-value store.
- Unit testing using the Topology Test Driver.

![topology.png](topology.png)

## Prerequisites

To compile and run this demo, you’ll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaUser>` to the `USER_TOPIC`. You can use the [Producer User](../specific-producers/kafka-streams-producer-user) for this.
- Start the Kafka Streams application.

Alternatively, to run the application with Docker, use the following command:

```console
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Schema Registry
- Control Center
- Producer User
- Kafka Streams Schedule
