# Consumer Circuit Breaker

This module demonstrates the following:

- The use of the Kafka Clients consumer API.
- The use of Apache Avro
- Unit testing using a Mock consumer.

This module performs the following tasks:

- Consume records of type `<String, KafkaUser>` from a topic named `USER_TOPIC`.
- Handle deserialization exceptions and seek the poison pill.
- If a poison pill is found in the middle of a batch of good records, the `poll()` method will return the good records
  in the first loop and then throw the deserialization exception in the second loop.
- To ensure no good records are lost, the module seeks to the next offset.

## Prerequisites

To compile and run this demo, you will need the following:

- Java 21
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaUser>` to a topic named `USER_TOPIC`. You can use the [producer Avro Specific](../../kafka-producer-quickstarts/kafka-producer-avro-specific) to do this.
- Make sure a deserialization exception will occur by either:
    - Deleting the Avro schema from the schema registry.
    - Producing a String message in the middle of Avro messages.
- Start the consumer.

To run the application in Docker, use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Kafka broker (KRaft mode)
- 1 Schema registry
- 1 Control Center
- 1 producer Avro Specific
- 1 Circuit Breaker consumer
