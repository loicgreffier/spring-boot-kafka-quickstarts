# Kafka Streams Schedule

This module streams records of type `<String, Order>` from the `ORDER_TOPIC` and counts the orders by customer in a key-value store.
Every minute of stream time, the counters are forwarded as `<String, Long>` records to the `ORDER_SCHEDULE_TOPIC`. Every 2 minutes of wall clock time, the counters are reset to 0.

It demonstrates the following:

- How to use the Kafka Streams Processor API, including `process()` and `schedule()`.
- How to use the processor context and schedule tasks based on wall clock time and stream time.
- How to create a key-value store.
- How to write unit tests with Topology Test Driver.

## Prerequisites

To compile and run this demo, you'll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/get-started/platform-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, Order>` to the `ORDER_TOPIC`. You can use the [Producer Order](../specific-producers/kafka-streams-producer-order) for this.
- Start the Kafka Streams application.

Alternatively, to run everything at once using Docker, run:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Schema Registry
- Control Center
- Producer Order
- Kafka Streams Schedule
