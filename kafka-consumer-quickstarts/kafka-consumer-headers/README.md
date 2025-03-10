# Consumer Headers

This module demonstrates the following:

- The use of the Kafka Clients consumer API.
- The use of headers in Kafka records.
- Unit testing using a Mock consumer.

This module consumes records of type `<String, String>` with headers from a topic named `STRING_TOPIC`.

## Prerequisites

To compile and run this demo, you will need the following:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, String>` with headers named `id` and `message` to a topic named `STRING_TOPIC`. You can use the [producer headers](../../kafka-producer-quickstarts/kafka-producer-headers) to do this.
- Start the consumer.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Control Center
- 1 producer Headers
- 1 consumer Headers
