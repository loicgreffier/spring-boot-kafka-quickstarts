# Kafka Streams Process

This module demonstrates:

- The use of the Kafka Streams DSL: `process()`
- The use of the Processor API
  - The access to the processor context
  - The creation of a state store and its connection to the processor
- The use of unit tests with Topology Test Driver.

This module does:

- Stream records of type <String,KafkaPerson> from a topic named PERSON_TOPIC.
- Process the stream with a custom processor that:
  - transforms each record by changing the key to the last name and enriching the value with metadata such as topic, partition, and offset.
  - counts each occurrence of the record by last name and stores the result in a state store.
- Write back the result into a new topic named PERSON_PROCESS_TOPIC.

![topology.png](topology.png)

## Requirements

To compile and run this demo, you will need the following:

- Java 17
- Maven
- Docker

## Running the Application

To run the application manually, please follow the steps below:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type <String,KafkaPerson> to a topic named PERSON_TOPIC. You can use the [producer person](../specific-producers/kafka-streams-producer-person) to do this.
- Start the Kafka Streams.

To run the application in Docker, please use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Zookeeper
- 1 Kafka broker
- 1 Schema registry
- 1 Control Center
- 1 producer person
- 1 Kafka Streams process
