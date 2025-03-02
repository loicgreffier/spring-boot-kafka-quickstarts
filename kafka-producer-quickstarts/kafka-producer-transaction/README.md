# Producer Transaction

This module demonstrates the following:

- The use of the Kafka Clients producer API.
- The use of Kafka transactions.
- Unit testing using a Mock producer.

This module produces transactional records of type `<String, String>` to two topics named `FIRST_STRING_TOPIC`
and `SECOND_STRING_TOPIC`.

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
- 1 Control Center
- 1 producer Transaction
