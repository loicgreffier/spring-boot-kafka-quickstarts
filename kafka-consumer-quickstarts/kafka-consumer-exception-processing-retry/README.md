# Consumer Exception Processing Retry

This module consumes records of type `<String, String>` from the `STRING_TOPIC` and implements a retry mechanism to handle exceptions that occur while processing records.
It demonstrates the following:

- How to use the Kafka Clients consumer API.
- Unit testing with a mock consumer.

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, String>` to the `STRING_TOPIC`. You can use the [Producer Simple](../../kafka-producer-quickstarts/kafka-producer-simple) for this.
- Start the consumer.

Alternatively, to run everything at once using Docker, run:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Control Center
- Producer Simple
- Consumer Exception Processing Retry
