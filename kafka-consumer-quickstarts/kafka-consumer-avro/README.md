# Consumer Avro

This module shows:
- Kafka Clients consumer API
- Apache Avro
- Unit tests with Mock consumer

This module does:
- consume <String,KafkaPerson> records from a topic named PERSON_TOPIC

## Requirements

To compile and run this demo you will need:
- Java 17
- Maven
- Docker

## Run the app

For manual run:
- start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in Docker
- produce <String,KafkaPerson> records to a topic named PERSON_TOPIC. The [producer Avro](../../kafka-producer-quickstarts/kafka-producer-avro) can be used
- start the consumer

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
- 1 producer Avro
- 1 consumer Avro
