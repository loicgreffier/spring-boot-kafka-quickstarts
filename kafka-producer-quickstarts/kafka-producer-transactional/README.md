# Producer Transactional

This module shows:
- Kafka Clients producer API
- Kafka transactions
- unit tests with Mock producer

This module does:
- produce <String,String> transactional records to two topic named FIRST_STRING_TOPIC and SECOND_STRING_TOPIC

## Requirements

To compile and run this demo you will need:
- Java 17
- Maven
- Docker

## Run the app

For manual run:
- start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in Docker
- start the producer

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