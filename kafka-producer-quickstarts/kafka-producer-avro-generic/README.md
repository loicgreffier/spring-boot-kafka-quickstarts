# Producer Avro Generic

This module demonstrates the following:

- The use of the Kafka Clients producer API.
- The use of Apache Avro and generic records.
- Unit testing using a Mock producer.

This module produces records of type `<String, GenericRecord>` to a topic named `USER_TOPIC`.

## Requirements

To compile and run this demo, you will need the following:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually, please follow the steps below:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Start the producer.

To run the application in Docker, please use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka broker KRaft
- 1 Schema registry
- 1 Control Center
- 1 producer Avro Generic
