# Producer Avro Specific

This module demonstrates the following:

- The usage of the Kafka Clients producer DSL.
- The usage of Apache Avro and specific records.
- The usage of unit tests with a Mock producer.

This module produces records of type `<String, KafkaPerson>` to a topic named `PERSON_TOPIC`.

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
- Start the producer.

To run the application in Docker, please use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Zookeeper
- 1 Kafka broker
- 1 Schema registry
- 1 Control Center
- 1 producer Avro Specific
