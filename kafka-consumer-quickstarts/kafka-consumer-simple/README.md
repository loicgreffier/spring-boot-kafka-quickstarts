# Consumer Simple

This module demonstrates:

- The use of the Kafka Clients consumer DSL.
- The use of unit tests with a Mock consumer.

This module consumes records of type <String,String> from a topic named STRING_TOPIC.

## Requirements

To compile and run this demo, you will need the following:

- Java 17
- Maven
- Docker

## Running the Application

To run the application manually, please follow the steps below:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type <String,String> to a topic named STRING_TOPIC. You can use the [producer simple](../../kafka-producer-quickstarts/kafka-producer-simple) to do this.
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
- 1 consumer simple
