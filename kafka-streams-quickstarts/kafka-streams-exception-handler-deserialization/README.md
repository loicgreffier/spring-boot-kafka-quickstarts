# Kafka Streams Deserialization Exception Handler

This module streams records of type `<String, Order>` from the `ORDER_TOPIC` and handles deserialization exceptions.

It demonstrates the following:

- How to use the Kafka Streams configuration `deserialization.exception.handler` to handle deserialization exceptions.
- How to implement a custom deserialization exception handler.
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
- To activate the custom deserialization exception handler, produce a record with a value that cannot be deserialized to the `Order` type (e.g., a value that is not in Avro format) to the `ORDER_TOPIC`. You can use the Control Center for this.

Alternatively, to run everything at once using Docker, run:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Schema Registry
- Control Center
- Producer Order
- Kafka Streams Exception Handler Deserialization
