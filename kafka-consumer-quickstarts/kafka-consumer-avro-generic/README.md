# Consumer Avro Generic

This module demonstrates the following:

- The use of the Kafka Clients consumer API.
- The use of Apache Avro and generic records.
- Unit testing using a Mock consumer.

This module consumes records of type `<String, GenericRecord>` from a topic named `USER_TOPIC`.

## Prerequisites

To compile and run this demo, you will need the following:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, GenericRecord>` to a topic named `USER_TOPIC`. You can use the [producer Avro Generic](../../kafka-producer-quickstarts/kafka-producer-avro-generic) to do this.
- Start the consumer.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Schema registry
- 1 Control Center
- 1 producer Avro Generic
- 1 consumer Avro Generic
