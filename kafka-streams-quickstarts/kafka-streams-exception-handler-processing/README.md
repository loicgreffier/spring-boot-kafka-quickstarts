# Kafka Streams Processing Exception Handler

This module demonstrates the following:

- The usage of the Kafka Streams configuration `processing.exception.handler` to handle processing exceptions that occur in 
the DSL operations, the processor API, and the punctuations.
- The implementation of a custom processing exception handler.
- Unit testing using Topology Test Driver.

In this module, records of type `<String, KafkaPerson>` are streamed from a topic named `PERSON_TOPIC`.
The following tasks are performed:

1. Log the received records.
2. Map the values of the records and throw a processing exception if the birthdate is negative.
3. Process the values of the records and throw a processing exception if the last name or the first name is empty.
4. Schedule a punctuation every 1 minute to throw a processing exception.
5. Write the records as they are to a new topic named `PERSON_PROCESSING_EXCEPTION_HANDLER_TOPIC`.

The custom processing exception handler is invoked when any of these exceptions are thrown.
It logs the exceptions and continues processing.

![topology.png](topology.png)

## Requirements

To compile and run this demo, you will need the following:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually, please follow the steps below:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaPerson>` to a topic named `PERSON_TOPIC`. You can use the [producer person](../specific-producers/kafka-streams-producer-person) to do this.
- Start the Kafka Streams.

To run the application in Docker, please use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka broker KRaft
- 1 Schema registry
- 1 Control Center
- 1 producer Person
- 1 Kafka Streams Exception Handler Processing