# Kafka Streams Branch

This module shows:
- Kafka Streams API: `branch()`, `peek()`
- unit tests with Topology Test Driver

This module does:
- stream <String,KafkaPerson> records from a topic named PERSON_TOPIC
- split the records to different topics according to criteria:
  - persons with a last name starting with A to a topic named PERSON_BRANCH_A_TOPIC. Both first name and last name are set to uppercase.
  - persons with a last name starting with B to a topic named PERSON_BRANCH_B_TOPIC.
  - other persons to a topic named PERSON_BRANCH_DEFAULT_TOPIC.

## Requirements

To compile and run this demo you will need:
- Java 17
- Maven
- Docker

## Run the app

For manual run:
- start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in Docker
- produce <String,KafkaPerson> records to a topic named PERSON_TOPIC. The [producer person](../specific-producers/kafka-streams-producer-person) can be used
- start the Kafka Streams

For Docker run:
- start the provided docker-compose 

```
docker compose up -d
```

The docker compose runs:
- 1 Zookeeper
- 1 Kafka broker
- 1 Schema registry
- 1 Control Center
- 1 producer person
- 1 Kafka Streams branch
