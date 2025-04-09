# Consumer Simple

This module demonstrates the following:

- How to use the Kafka Clients consumer API.
- Unit testing with a mock consumer.

This module consumes records of type `<String, String>` from a topic named `STRING_TOPIC`.

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, String>` to a topic named `STRING_TOPIC`. You can use the [Producer Simple](../../kafka-producer-quickstarts/kafka-producer-simple) to do this.
- Start the consumer.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka Broker (KRaft mode)
- 1 Control Center
- 1 producer Simple
- 1 consumer Simple
