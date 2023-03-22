# Kafka Streams Merge

This module demonstrates:

- The use of the Kafka Streams API: `merge()`, `peek()`.
- The use of unit tests with Topology Test Driver.

This module does:

- Stream records of type <String,KafkaPerson> from topics named PERSON_TOPIC and PERSON_TOPIC_TWO.
- Merge the two streams.
- Write the merged stream into a new topic named PERSON_MERGE_TOPIC.

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
- 1 Kafka Streams merge