# Consumer Retry External System

This module demonstrates the following:

- The use of the Kafka Clients consumer API.
- Unit testing using a Mock consumer.

This module performs the following tasks:

- Consumes records of type `<String, String>` from a topic named `STRING_TOPIC`.
- Sends the records to a fake external system.
- Implements a strong retry mechanism to handle failures in the external system. If the call to the external system
  fails, the module retries the operation using the retry mechanism.

## Prerequisites

To compile and run this demo, you will need the following:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, String>` to a topic named `STRING_TOPIC`. You can use the [producer simple](../../kafka-producer-quickstarts/kafka-producer-simple) to do this.
- Start the consumer.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Control Center
- 1 producer Simple
- 1 consumer Retry External System
