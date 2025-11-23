# Consumer Avro Specific

This module consumes records of type `<String, KafkaUser>` from the `USER_TOPIC`.

It demonstrates the following:

- How to use the Kafka Clients consumer API.
- How to work with Apache Avro and specific records.
- Unit testing with a mock consumer.

## Prerequisites

To compile and run this demo, you'll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaUser>` to the `USER_TOPIC`. You can use the [Producer Avro Specific](../../kafka-producer-quickstarts/kafka-producer-avro-specific) for this.
- Start the consumer.

Alternatively, to run everything at once using Docker, run:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Schema Registry
- Control Center
- Producer Avro Specific
- Consumer Avro Specific
