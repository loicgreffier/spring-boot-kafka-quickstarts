[![GitHub Build](https://img.shields.io/github/actions/workflow/status/loicgreffier/spring-boot-kafka-quickstarts/continuous_integration.yml?branch=main&logo=github&style=for-the-badge)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts/actions/workflows/continuous_integration.yml)
[![GitHub Stars](https://img.shields.io/github/stars/loicgreffier/spring-boot-kafka-quickstarts?logo=github&style=for-the-badge)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts)
[![GitHub Watch](https://img.shields.io/github/watchers/loicgreffier/spring-boot-kafka-quickstarts?logo=github&style=for-the-badge)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts)
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

| Module                                                                    | Library       | API      | Additional Content                |
|---------------------------------------------------------------------------|---------------|----------|-----------------------------------|
| [Avro](/kafka-producer-quickstarts/kafka-producer-avro)                   | Kafka Clients | Producer | Apache Avro, Mock producer        |
| [Simple](/kafka-producer-quickstarts/kafka-producer-simple)               | Kafka Clients | Producer | Header, Mock producer             |
| [Transactional](/kafka-producer-quickstarts/kafka-producer-transactional) | Kafka Clients | Producer | Kafka transactions, Mock producer |

### Consumers

| Module                                                                                    | Library       | API      | Additional Content                                                     |
|:------------------------------------------------------------------------------------------|---------------|----------|------------------------------------------------------------------------|
| [Avro](/kafka-consumer-quickstarts/kafka-consumer-avro)                                   | Kafka Clients | Consumer | Apache Avro, Mock consumer                                             |
| [Circuit breaker](/kafka-consumer-quickstarts/kafka-consumer-circuit-breaker)             | Kafka Clients | Consumer | Deserialization/poison pill error handling, Mock consumer              |
| [Retry external system](/kafka-consumer-quickstarts/kafka-consumer-retry-external-system) | Kafka Clients | Consumer | External system interfacing with strong retry mechanism, Mock consumer |
| [Simple](/kafka-consumer-quickstarts/kafka-consumer-simple)                               | Kafka Clients | Consumer | Header, Mock consumer                                                  |
| [Transactional](/kafka-consumer-quickstarts/kafka-consumer-transactional)                 | Kafka Clients | Consumer | Kafka transactions, Isolation level                                    |

### Streams

#### Source

| Module                                                                            | Library       | Operations      | Note                                                                 |
|:----------------------------------------------------------------------------------|---------------|-----------------|----------------------------------------------------------------------|
| [Global table](/kafka-streams-quickstarts/kafka-streams-join-stream-global-table) | Kafka Streams | `globalTable()` | All KStream-GlobalKTable join examples contain global table sourcing |
| [Table](/kafka-streams-quickstarts/kafka-streams-join-stream-table)               | Kafka Streams | `table()`       | All KStream-KTable join examples contain table sourcing              |
| [Stream](/kafka-streams-quickstarts/kafka-streams-map)                            | Kafka Streams | `stream()`      | All examples contain stream sourcing                                 |

#### Stateless

| Module                                                                    | Library       | Operations                                                      | Additional Content   |
|:--------------------------------------------------------------------------|---------------|-----------------------------------------------------------------|----------------------|
| [Branch](/kafka-streams-quickstarts/kafka-streams-branch)                 | Kafka Streams | `branch()`, `peek()`                                            | Topology Test Driver |
| [Cogroup](/kafka-streams-quickstarts/kafka-streams-cogroup)               | Kafka Streams | `cogroup()`, `groupBy()`, `aggregate()`, `toStream()`, `peek()` | Topology Test Driver |
| [Filter](/kafka-streams-quickstarts/kafka-streams-filter)                 | Kafka Streams | `filter()`, `filterNot()`, `peek()`                             | Topology Test Driver |
| [Flatmap](/kafka-streams-quickstarts/kafka-streams-flatmap)               | Kafka Streams | `flatMap()`, `peek()`                                           | Topology Test Driver |
| [Flatmap values](/kafka-streams-quickstarts/kafka-streams-flatmap-values) | Kafka Streams | `flatMapValues()`, `peek()`                                     | Topology Test Driver |
| [Foreach](/kafka-streams-quickstarts/kafka-streams-foreach)               | Kafka Streams | `foreach()`                                                     |                      |
| [Map](/kafka-streams-quickstarts/kafka-streams-map)                       | Kafka Streams | `map()`, `peek()`                                               | Topology Test Driver |
| [Map values](/kafka-streams-quickstarts/kafka-streams-map-values)         | Kafka Streams | `mapValues()`, `peek()`                                         | Topology Test Driver |
| [Merge](/kafka-streams-quickstarts/kafka-streams-merge)                   | Kafka Streams | `merge()`, `peek()`                                             | Topology Test Driver |
| [Print](/kafka-streams-quickstarts/kafka-streams-print)                   | Kafka Streams | `print()`, `peek()`                                             |                      |
| [Repartition](/kafka-streams-quickstarts/kafka-streams-repartition)       | Kafka Streams | `repartition()`, `peek()`                                       | Topology Test Driver |
| [Select key](/kafka-streams-quickstarts/kafka-streams-select-key)         | Kafka Streams | `selectKey()`, `peek()`                                         | Topology Test Driver |

#### Aggregate

| Module                                                          | Library       | Operations                                                           | Additional Content   |
|:----------------------------------------------------------------|---------------|----------------------------------------------------------------------|----------------------|
| [Aggregate](/kafka-streams-quickstarts/kafka-streams-aggregate) | Kafka Streams | `aggregate()`, `groupByKey()`, `selectKey()`, `toStream()`, `peek()` | Topology Test Driver |
| [Count](/kafka-streams-quickstarts/kafka-streams-count)         | Kafka Streams | `count()`, `groupBy()`, `toStream()`, `peek()`                       | Topology Test Driver |
| [Reduce](/kafka-streams-quickstarts/kafka-streams-reduce)       | Kafka Streams | `reduce()`, `groupBy()`, `toStream()`, `peek()`                      | Topology Test Driver |

#### Join

| Module                                                                                                  | Library       | Operations                                                         | Additional Content                    |
|:--------------------------------------------------------------------------------------------------------|---------------|--------------------------------------------------------------------|---------------------------------------|
| [Join Stream-Global Table](/kafka-streams-quickstarts/kafka-streams-join-stream-global-table)           | Kafka Streams | `join()` between KStream and GlobalKTable, `peek()`                | Topology Test Driver                  |
| [Join Stream-Stream](/kafka-streams-quickstarts/kafka-streams-join-stream-stream)                       | Kafka Streams | `join()` between KStream and KStream, `selectKey()`, `peek()`      | Sliding Windows, Topology Test Driver |
| [Join Stream-Table](/kafka-streams-quickstarts/kafka-streams-join-stream-table)                         | Kafka Streams | `join()` between KStream and KTable, `selectKey()`, `peek()`       | Topology Test Driver                  |
| [Left join Stream-Global Table](/kafka-streams-quickstarts/kafka-streams-left-join-stream-global-table) | Kafka Streams | `leftJoin()` between KStream and GlobalKTable, `peek()`            | Topology Test Driver                  |
| [Left join Stream-Stream](/kafka-streams-quickstarts/kafka-streams-left-join-stream-stream)             | Kafka Streams | `leftJoin()` between KStream and KStream, `selectKey()`, `peek()`  | Sliding Windows, Topology Test Driver |
| [Left join Stream-Table](/kafka-streams-quickstarts/kafka-streams-left-join-stream-table)               | Kafka Streams | `leftJoin()` between KStream and KTable, `selectKey()`, `peek()`   | Topology Test Driver                  |
| [Outer join Stream-Stream](/kafka-streams-quickstarts/kafka-streams-outer-join-stream-stream)           | Kafka Streams | `outerJoin()` between KStream and KStream, `selectKey()`, `peek()` | Sliding Windows, Topology Test Driver |

#### Windowing

| Module                                                                             | Library       | Operations | Note                                                           |
|:-----------------------------------------------------------------------------------|---------------|------------|----------------------------------------------------------------|
| [Sliding time window](/kafka-streams-quickstarts/kafka-streams-join-stream-stream) | Kafka Streams |            | All KStream-KStream join examples contain sliding time windows |
