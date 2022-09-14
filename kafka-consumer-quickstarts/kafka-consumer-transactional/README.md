# Consumer Transactional

This module shows:
- Kafka Clients consumer API
- isolation level

This module does:
- consume <String,String> records from topics named FIRST_STRING_TOPIC and SECOND_STRING_TOPIC with an _isolation_level_ set to
_read_committed_

## Requirements

To compile and run this demo you will need:
- Java 17
- Maven
- Docker

## Run the app

For manual run:
- start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in Docker
- produce <String,String> transactional records to topics named FIRST_STRING_TOPIC and SECOND_STRING_TOPIC. The [producer transactional](../../kafka-producer-quickstarts/kafka-producer-transactional) can be used
- start the consumer

For Docker run:
- start the provided docker-compose 

```
docker compose up -d
```

The docker compose runs:
- 1 Zookeeper
- 1 Kafka broker
- 1 Control Center
- 1 producer transactional
- 1 consumer transactional
