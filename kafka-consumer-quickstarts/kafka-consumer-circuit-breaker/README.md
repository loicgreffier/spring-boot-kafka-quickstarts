# Consumer Circuit Breaker

This module demonstrates:

- The use of the Kafka Clients consumer DSL.
- The use of Apache Avro.
- The use of unit tests with a Mock consumer.

This module does:

- Consume records of type <String,KafkaPerson> from a topic named PERSON_TOPIC.
- Handle deserialization exceptions and seek the poison pill. If a poison pill is found in the middle of a batch of good records, the `poll()` method will return the good records in the first loop, and then throw the deserialization exception in the second loop. We handle this by seeking to the next offset. No good records will be lost.

## Requirements

To compile and run this demo, you will need the following:

- Java 17
- Maven
- Docker

## Running the Application

To run the application manually, please follow the steps below:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type <String,KafkaPerson> to a topic named PERSON_TOPIC. You can use the [producer Avro](../../kafka-producer-quickstarts/kafka-producer-avro) to do this.
- Make sure a deserialization exception will occur by either:
  - Deleting the Avro schema from the schema registry.
  - Producing a String message in the middle of Avro messages.
- Start the consumer.

To run the application in Docker, please use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Zookeeper
- 1 Kafka broker
- 1 Schema registry
- 1 Control Center
- 1 producer Avro
- 1 Circuit Breaker consumer
