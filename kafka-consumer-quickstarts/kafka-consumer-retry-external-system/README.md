# Getting Started

This module shows:
- Kafka Clients consumer API
- Unit tests with Mock consumer

This module does:
- consume <String,String> records from a topic named STRING_TOPIC
- send records to a fake external system, and perform retries through a strong retry mechanism if the call did fail

## Requirements

To compile and run this demo you will need:
- Java 17
- Maven
- Docker

## Run the app

For manual run:
- start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in Docker
- produce <String,String> records to a topic named STRING_TOPIC. The [producer simple](../../kafka-producer-quickstarts/kafka-producer-simple) can be used
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
- 1 producer simple
- 1 consumer retry external system
