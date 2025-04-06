# Consumer Headers

This module consumes records of type `<String, String>` with headers from a topic named `STRING_TOPIC`.
It demonstrates the following:
- Use of the Kafka Clients consumer API
- Use of headers in Kafka records
- Unit testing with a mock consumer

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, String>` with a header named `message` to the `STRING_TOPIC`. You can use the [producer Headers](../../kafka-producer-quickstarts/kafka-producer-headers) for this.
- Start the consumer.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command starts the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Control Center
- 1 Producer Headers
- 1 Consumer Headers
