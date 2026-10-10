# Producer Headers

This module produces orders serialized as JSON strings, of type `<String, String>`, to the `ORDER_JSON_TOPIC`.
Each record carries a `correlationId` header and an `eventType` header.

It demonstrates the following:

- How to use the Kafka Clients producer API.
- How to use headers in Kafka records.
- How to write unit tests with a mock producer.

## Prerequisites

To compile and run this demo, you'll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/get-started/platform-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Start the producer.

Alternatively, to run everything at once using Docker, run:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Control Center
- Producer Headers
