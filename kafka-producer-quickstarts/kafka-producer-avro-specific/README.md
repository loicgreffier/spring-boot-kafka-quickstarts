# Producer Avro Specific

This module demonstrates the following:

- The use of the Kafka Clients producer API.
- The use of Apache Avro and specific records.
- Unit testing using a Mock producer.

This module produces records of type `<String, KafkaUser>` to a topic named `USER_TOPIC`.

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Start the producer.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Schema Registry
- 1 Control Center
- 1 producer Avro Specific
