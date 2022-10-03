# Kafka Streams Cogroup

This module shows:
- Kafka Streams API: `cogroup()`, `groupBy()`, `aggregate()`, `peek()`
- unit tests with Topology Test Driver

This module does:
- stream <String,KafkaPerson> records from topics named PERSON_TOPIC and PERSON_TOPIC_TWO
- group each stream by last name
- cogroup all streams with an aggregator. 
The aggregator combines each KafkaPerson with the same last name into KafkaPersonGroup records and aggregate first names by last name such as

```
{"firstNameByLastName":{"Last name 1":{"First name 1", "First name 2", "First name 3")}}
{"firstNameByLastName":{"Last name 2":{"First name 4", "First name 5", "First name 6")}}
{"firstNameByLastName":{"Last name 3":{"First name 7", "First name 8", "First name 9")}}
```

## Requirements

To compile and run this demo you will need:
- Java 17
- Maven
- Docker

## Run the app

For manual run:
- start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in Docker
- produce <String,KafkaPerson> records to topics named PERSON_TOPIC and PERSON_TOPIC_TWO. The [producer person](../specific-producers/kafka-streams-producer-person) can be used
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
- 1 Kafka Streams cogroup
