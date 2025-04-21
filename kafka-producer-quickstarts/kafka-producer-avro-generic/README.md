# Producer Avro Generic

This module produces records of type `<String, GenericRecord>` to the `USER_TOPIC`.
It demonstrates the following:

- The use of the Kafka Clients producer API.
- The use of Apache Avro and generic records.
- Unit testing using a Mock producer.

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Start the producer.

Alternatively, to run the application with Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka Broker (KRaft mode)
- 1 Schema Registry
- 1 Control Center
- 1 Producer Avro Generic
