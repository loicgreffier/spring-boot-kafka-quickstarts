# Kafka Streams Processing Exception Handler

This module demonstrates the following:

- The use of the Kafka Streams configuration `processing.exception.handler` to handle processing exceptions that occur in 
DSL operations and the processor API.
- The implementation of a custom processing exception handler.
- Unit testing using Topology Test Driver.

In this module, records of type `<String, KafkaUser>` are streamed from a topic named `USER_TOPIC`.
The following tasks are performed:

1. Log the received records.
2. Map the values of the records and throw a processing exception if the birthdate is negative.
3. Process the values of the records and throw a processing exception if the last name or the first name is empty.
4. Schedule a punctuation every 1 minute to throw a processing exception.
5. Write the records as they are to a new topic named `USER_PROCESSING_EXCEPTION_HANDLER_TOPIC`.

The custom processing exception handler is invoked when any of these exceptions are thrown.
It logs the exceptions and continues processing for `IllegalArgumentException`, but fails the processing for other exceptions.

![topology.png](topology.png)

## Prerequisites

To compile and run this demo, you’ll need:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaUser>` to the `USER_TOPIC`. You can use the [producer user](../specific-producers/kafka-streams-producer-user) for this.
- Start the Kafka Streams application.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command starts the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Schema Registry
- 1 Control Center
- 1 Producer User
- 1 Kafka Streams Exception Handler Processing
