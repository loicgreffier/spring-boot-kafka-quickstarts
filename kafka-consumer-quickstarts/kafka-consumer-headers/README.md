# Consumer Headers

This module consumes records of type `<String, String>` from the `STRING_TOPIC` along with their associated headers.
It demonstrates the following:

- How to use the Kafka Clients consumer API.
- How to use headers in Kafka records.
- Unit testing with a mock consumer.

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, String>` with headers named `id` and `message` to the `STRING_TOPIC`. You can use the [Producer Headers](../../kafka-producer-quickstarts/kafka-producer-headers) for this.
- Start the consumer.

Alternatively, to run the application with Docker, use the following command:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Control Center
- Producer Headers
- Consumer Headers
