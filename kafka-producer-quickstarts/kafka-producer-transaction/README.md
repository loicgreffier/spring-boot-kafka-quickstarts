# Producer Transaction

This module produces transactional records of type `<String, String>` to two topics: `FIRST_STRING_TOPIC`
and `SECOND_STRING_TOPIC`.
It demonstrates the following:

- How to use the Kafka Clients producer API.
- How to use Kafka transactions.
- Unit testing with a mock producer.

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
- 1 Control Center
- 1 Producer Transaction
