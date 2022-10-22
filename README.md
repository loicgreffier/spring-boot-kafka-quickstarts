[![GitHub Build](https://img.shields.io/github/workflow/status/loicgreffier/spring-boot-kafka-quickstarts/continuous-integration/main?logo=github&style=for-the-badge)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts/actions/workflows/continuous_integration.yml)
[![GitHub Stars](https://img.shields.io/github/stars/loicgreffier/spring-boot-kafka-quickstarts?logo=github&style=for-the-badge)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts)
[![Docker Pulls](https://img.shields.io/docker/pulls/loicgreffier/spring-boot-kafka-quickstarts?label=Pulls&logo=docker&style=for-the-badge)](https://hub.docker.com/r/loicgreffier/spring-boot-kafka-quickstarts/tags)
[![Docker Stars](https://img.shields.io/docker/stars/loicgreffier/spring-boot-kafka-quickstarts?label=Stars&logo=docker&style=for-the-badge)](https://hub.docker.com/r/loicgreffier/spring-boot-kafka-quickstarts)

# Spring Boot and Kafka quickstarts

This repository contains a set of code samples around Kafka Clients, Kafka Streams and Spring Boot. 

## Requirements

- Java 17
- Maven
- Docker 

## Quickstarts list

### Producers

| Module                                                                             | Library       | API      | Content                           |
|------------------------------------------------------------------------------------|---------------|----------|-----------------------------------|
| [Producer Avro](/kafka-producer-quickstarts/kafka-producer-avro)                   | Kafka Clients | Producer | Apache Avro, Mock producer        |
| [Producer simple](/kafka-producer-quickstarts/kafka-producer-simple)               | Kafka Clients | Producer | Mock producer                     |
| [Producer transactional](/kafka-producer-quickstarts/kafka-producer-transactional) | Kafka Clients | Producer | Kafka transactions, Mock producer |

### Consumers

| Module                                                                                             | Library       | API      | Content                                                                |
|:---------------------------------------------------------------------------------------------------|---------------|----------|------------------------------------------------------------------------|
| [Consumer Avro](/kafka-consumer-quickstarts/kafka-consumer-avro)                                   | Kafka Clients | Consumer | Apache Avro, Mock consumer                                             |
| [Consumer circuit breaker](/kafka-consumer-quickstarts/kafka-consumer-circuit-breaker)             | Kafka Clients | Consumer | Deserialization/poison pill error handling, Mock consumer              |
| [Consumer retry external system](/kafka-consumer-quickstarts/kafka-consumer-retry-external-system) | Kafka Clients | Consumer | External system interfacing with strong retry mechanism, Mock consumer |
| [Consumer simple](/kafka-consumer-quickstarts/kafka-consumer-simple)                               | Kafka Clients | Consumer | Mock consumer                                                          |
| [Consumer transactional](/kafka-consumer-quickstarts/kafka-consumer-transactional)                 | Kafka Clients | Consumer | Kafka transactions, Isolation level                                    |

### Streams

| Module                                                                                            | Library       | Operations                                                                                     | Additional Content   |
|:--------------------------------------------------------------------------------------------------|---------------|------------------------------------------------------------------------------------------------|----------------------|
| [Streams branch](/kafka-streams-quickstarts/kafka-streams-branch)                                 | Kafka Streams | `branch()`, `peek()`                                                                           | Topology Test Driver |
| [Streams cogroup](/kafka-streams-quickstarts/kafka-streams-cogroup)                               | Kafka Streams | `cogroup()`, `groupBy()`, `aggregate()`, `peek()`                                              | Topology Test Driver |
| [Streams filter](/kafka-streams-quickstarts/kafka-streams-filter)                                 | Kafka Streams | `filter()`, `filterNot()`, `peek()`                                                            | Topology Test Driver |
| [Streams flatmap](/kafka-streams-quickstarts/kafka-streams-flatmap)                               | Kafka Streams | `flatMap()`, `peek()`                                                                          | Topology Test Driver |
| [Streams flatmap values](/kafka-streams-quickstarts/kafka-streams-flatmap-values)                 | Kafka Streams | `flatMapValues()`, `peek()`                                                                    | Topology Test Driver |
| [Streams foreach](/kafka-streams-quickstarts/kafka-streams-foreach)                               | Kafka Streams | `foreach()`                                                                                    |                      |
| [Streams join stream table](/kafka-streams-quickstarts/kafka-streams-join-stream-table)           | Kafka Streams | `join()` between KStream and KTable, `selectKey()`, `repartition()`, `toTable()`, `peek()`     | Topology Test Driver |
| [Streams left join stream table](/kafka-streams-quickstarts/kafka-streams-left-join-stream-table) | Kafka Streams | `leftJoin()` between KStream and KTable, `selectKey()`, `repartition()`, `toTable()`, `peek()` | Topology Test Driver |
| [Streams map](/kafka-streams-quickstarts/kafka-streams-map)                                       | Kafka Streams | `map()`, `peek()`                                                                              | Topology Test Driver |
| [Streams map values](/kafka-streams-quickstarts/kafka-streams-map-values)                         | Kafka Streams | `mapValues()`, `peek()`                                                                        | Topology Test Driver |
| [Streams merge](/kafka-streams-quickstarts/kafka-streams-merge)                                   | Kafka Streams | `merge()`, `peek()`                                                                            | Topology Test Driver |
| [Streams repartition](/kafka-streams-quickstarts/kafka-streams-repartition)                       | Kafka Streams | `repartition()`, `peek()`                                                                      | Topology Test Driver |
| [Streams select key](/kafka-streams-quickstarts/kafka-streams-select-key)                         | Kafka Streams | `selectKey()`, `peek()`                                                                        | Topology Test Driver |