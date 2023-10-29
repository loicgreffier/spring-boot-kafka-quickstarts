# Consumer Retry External System

This module demonstrates the following:

- The usage of the Kafka Clients consumer DSL.
- The usage of unit tests with a Mock consumer.

This module performs the following tasks:

- Consumes records of type `<String, String>` from a topic named `STRING_TOPIC`.
- Sends the records to a fake external system.
- Implements a strong retry mechanism to handle failures in the external system. If the call to the external system
  fails, the module retries the operation using the retry mechanism.

## Requirements

To compile and run this demo, you will need the following:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually, please follow the steps below:

- Start
  a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp)
  in a Docker environment.
- Produce records of type `<String, String>` to a topic named `STRING_TOPIC`. You can use
  the [producer simple](../../kafka-producer-quickstarts/kafka-producer-simple) to do this.
- Start the consumer.

To run the application in Docker, please use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Zookeeper
- 1 Kafka broker
- 1 Control Center
- 1 producer simple
- 1 consumer retry external system
