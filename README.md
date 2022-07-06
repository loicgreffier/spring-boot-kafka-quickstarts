[![Docker Pulls](https://img.shields.io/docker/pulls/loicgreffier/spring-boot-kafka-quickstarts?logo=docker&style=for-the-badge)](https://hub.docker.com/r/loicgreffier/spring-boot-kafka-quickstarts/tags)
[![Docker Stars](https://img.shields.io/docker/stars/loicgreffier/spring-boot-kafka-quickstarts?logo=docker&style=for-the-badge)](https://hub.docker.com/r/loicgreffier/spring-boot-kafka-quickstarts/tags)

# Spring Boot and Kafka quickstarts

This repository contains a set of code samples around Kafka Clients, Kafka Streams and Spring Boot. 

## Requirements

- Java 17
- Maven
- Docker 

## Quickstarts list

### Producers

- [Producer simple](/kafka-producer-quickstarts/kafka-producer-simple): Kafka Clients, producer API, mock producer
- [Producer Avro](/kafka-producer-quickstarts/kafka-producer-avro): Kafka Clients, producer API, Apache Avro, mock producer

### Consumers

- [Consumer simple](/kafka-consumer-quickstarts/kafka-consumer-simple): Kafka Clients, consumer API, mock consumer
- [Consumer Avro](/kafka-consumer-quickstarts/kafka-consumer-avro): Kafka Clients, consumer API, Apache Avro, mock consumer
- [Consumer circuit breaker](/kafka-consumer-quickstarts/kafka-consumer-circuit-breaker): Kafka Clients, consumer API, deserialization/poison pill error handling, mock consumer
- [Consumer retry external system](/kafka-consumer-quickstarts/kafka-consumer-retry-external-system): Kafka Clients, consumer API, external system interfacing with strong retry mechanism, mock consumer

### Streams
