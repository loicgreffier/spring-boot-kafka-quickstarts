# Kafka Streams Left Join Stream Global Table

This module streams records of type `<String, Order>` from the `ORDER_TOPIC` and joins them by customer id with records of type `<String, Customer>` from the `CUSTOMER_TOPIC`.

It demonstrates the following:

- How to use the Kafka Streams DSL to join a `KStream` with a `GlobalKTable` using `leftJoin()` and `peek()`.
- How to write unit tests with Topology Test Driver.

## Prerequisites

To compile and run this demo, you'll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/get-started/platform-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, Customer>` to the `CUSTOMER_TOPIC`. You can use the [Producer Customer](../specific-producers/kafka-streams-producer-customer) for this.
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
- Producer Customer
- Producer Order
- Kafka Streams Left Join Stream Global Table
