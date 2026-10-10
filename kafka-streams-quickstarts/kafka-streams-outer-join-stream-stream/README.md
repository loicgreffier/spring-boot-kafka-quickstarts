# Kafka Streams Outer Join Stream Stream

This module streams records of type `<String, Order>` from the `ORDER_TOPIC` and records of type `<String, Payment>` from the `PAYMENT_TOPIC`,
and joins them by order id within a 5-minute window, allowing a 1-minute grace period for delayed records.
Both topics are keyed by order id, so the streams are joined without being re-keyed.

It demonstrates the following:

- How to use the Kafka Streams DSL to join two `KStream` using `outerJoin()` and `peek()`.
- How to write unit tests with Topology Test Driver.

## Prerequisites

To compile and run this demo, you'll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/get-started/platform-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, Order>` to the `ORDER_TOPIC` and records of type `<String, Payment>` to the `PAYMENT_TOPIC`. You can use the [Producer Order](../specific-producers/kafka-streams-producer-order) for this.
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
- Kafka Streams Outer Join Stream Stream
