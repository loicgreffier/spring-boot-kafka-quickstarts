# Spring Boot and Kafka quickstarts

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/loicgreffier/spring-boot-kafka-quickstarts/continuous_integration.yml?branch=main&logo=github&style=for-the-badge)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts/actions/workflows/continuous_integration.yml)
[![GitHub Stars](https://img.shields.io/github/stars/loicgreffier/spring-boot-kafka-quickstarts?logo=github&style=for-the-badge)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts)
[![GitHub Watch](https://img.shields.io/github/watchers/loicgreffier/spring-boot-kafka-quickstarts?logo=github&style=for-the-badge)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts)
[![Docker Pulls](https://img.shields.io/docker/pulls/loicgreffier/spring-boot-kafka-quickstarts?label=Pulls&logo=docker&style=for-the-badge)](https://hub.docker.com/r/loicgreffier/spring-boot-kafka-quickstarts/tags)
[![Docker Stars](https://img.shields.io/docker/stars/loicgreffier/spring-boot-kafka-quickstarts?label=Stars&logo=docker&style=for-the-badge)](https://hub.docker.com/r/loicgreffier/spring-boot-kafka-quickstarts)

This repository contains a set of code samples around Kafka Clients, Kafka Streams and Spring Boot. 

## Requirements

- Java 17
- Maven
- Docker 

## Quickstarts list

### Producers

| Module                                                                    | Library       | Main Concept      | Additional Content     |
|---------------------------------------------------------------------------|---------------|-------------------|------------------------|
| [Avro Generic](/kafka-producer-quickstarts/kafka-producer-avro-generic)   | Kafka Clients | Generic Avro      | Mock producer          |
| [Avro Specific](/kafka-producer-quickstarts/kafka-producer-avro-specific) | Kafka Clients | Specific Avro     | Mock producer          |
| [Simple](/kafka-producer-quickstarts/kafka-producer-simple)               | Kafka Clients | String Production | Headers, Mock producer |
| [Transactional](/kafka-producer-quickstarts/kafka-producer-transactional) | Kafka Clients | Transaction       | Mock producer          |

### Consumers

| Module                                                                                    | Library       | Main Concept                                            | Additional Content     |
|:------------------------------------------------------------------------------------------|---------------|---------------------------------------------------------|------------------------|
| [Avro Generic](/kafka-consumer-quickstarts/kafka-consumer-avro-generic)                   | Kafka Clients | Generic Avro                                            | Mock consumer          |
| [Avro Specific](/kafka-consumer-quickstarts/kafka-consumer-avro-specific)                 | Kafka Clients | Specific Avro                                           | Mock consumer          |
| [Circuit breaker](/kafka-consumer-quickstarts/kafka-consumer-circuit-breaker)             | Kafka Clients | Deserialization/poison pill error handling              | Mock consumer          |
| [Retry external system](/kafka-consumer-quickstarts/kafka-consumer-retry-external-system) | Kafka Clients | External system interfacing with strong retry mechanism | Mock consumer          |
| [Simple](/kafka-consumer-quickstarts/kafka-consumer-simple)                               | Kafka Clients | String Consumption                                      | Headers, Mock consumer |
| [Transactional](/kafka-consumer-quickstarts/kafka-consumer-transactional)                 | Kafka Clients | Isolation level                                         |                        |

### Streams

#### Source

| Module                                                                            | Library       | DSL             |
|:----------------------------------------------------------------------------------|---------------|-----------------|
| [Global table](/kafka-streams-quickstarts/kafka-streams-join-stream-global-table) | Kafka Streams | `globalTable()` |
| [Table](/kafka-streams-quickstarts/kafka-streams-join-stream-table)               | Kafka Streams | `table()`       |
| [Stream](/kafka-streams-quickstarts/kafka-streams-map)                            | Kafka Streams | `stream()`      |

#### Stateless

| Module                                                                    | Library       | DSL                       | Additional Content                                                       |
|:--------------------------------------------------------------------------|---------------|---------------------------|--------------------------------------------------------------------------|
| [Branch](/kafka-streams-quickstarts/kafka-streams-branch)                 | Kafka Streams | `branch()`                | `peek()`, Topology Test Driver                                           |
| [Cogroup](/kafka-streams-quickstarts/kafka-streams-cogroup)               | Kafka Streams | `cogroup()`               | `groupBy()`, `aggregate()`, `toStream()`, `peek()`, Topology Test Driver |
| [Filter](/kafka-streams-quickstarts/kafka-streams-filter)                 | Kafka Streams | `filter()`, `filterNot()` | `peek()`, Topology Test Driver                                           |
| [Flatmap](/kafka-streams-quickstarts/kafka-streams-flatmap)               | Kafka Streams | `flatMap()`               | `peek()`, Topology Test Driver                                           |
| [Flatmap values](/kafka-streams-quickstarts/kafka-streams-flatmap-values) | Kafka Streams | `flatMapValues()`         | `peek()`, Topology Test Driver                                           |
| [Foreach](/kafka-streams-quickstarts/kafka-streams-foreach)               | Kafka Streams | `foreach()`               |                                                                          |
| [Map](/kafka-streams-quickstarts/kafka-streams-map)                       | Kafka Streams | `map()`                   | `peek()`, Topology Test Driver                                           |
| [Map values](/kafka-streams-quickstarts/kafka-streams-map-values)         | Kafka Streams | `mapValues()`             | `peek()`, Topology Test Driver                                           |
| [Merge](/kafka-streams-quickstarts/kafka-streams-merge)                   | Kafka Streams | `merge()`                 | `peek()`, Topology Test Driver                                           |
| [Print](/kafka-streams-quickstarts/kafka-streams-print)                   | Kafka Streams | `print()`                 | `peek()`                                                                 |
| [Repartition](/kafka-streams-quickstarts/kafka-streams-repartition)       | Kafka Streams | `repartition()`           | `peek()`, Topology Test Driver                                           |
| [Select key](/kafka-streams-quickstarts/kafka-streams-select-key)         | Kafka Streams | `selectKey()`             | `peek()`, Topology Test Driver                                           |

#### Aggregate

| Module                                                                                          | Library       | DSL                                       | Additional Content                                                          |
|:------------------------------------------------------------------------------------------------|---------------|-------------------------------------------|-----------------------------------------------------------------------------|
| [Aggregate](/kafka-streams-quickstarts/kafka-streams-aggregate)                                 | Kafka Streams | `aggregate()`                             | `groupByKey()`, `selectKey()`, `toStream()`, `peek()`, Topology Test Driver |
| [Aggregate Tumbling Window](/kafka-streams-quickstarts/kafka-streams-aggregate-tumbling-window) | Kafka Streams | `aggregate()`, `windowedBy()`             | `groupByKey()`, `selectKey()`, `toStream()`, `peek()`, Topology Test Driver |
| [Aggregate Hopping Window](/kafka-streams-quickstarts/kafka-streams-aggregate-hopping-window)   | Kafka Streams | `aggregate()`, `windowedBy().advanceBy()` | `groupByKey()`, `selectKey()`, `toStream()`, `peek()`, Topology Test Driver |
| [Average](/kafka-streams-quickstarts/kafka-streams-average)                                     | Kafka Streams | `aggregate()`                             | `groupBy()`, `mapValues()`, `toStream()`, `peek()`, Topology Test Driver    |
| [Count](/kafka-streams-quickstarts/kafka-streams-count)                                         | Kafka Streams | `count()`                                 | `groupBy()`, `toStream()`, `peek()`, Topology Test Driver                   |
| [Reduce](/kafka-streams-quickstarts/kafka-streams-reduce)                                       | Kafka Streams | `reduce()`                                | `groupBy()`, `toStream()`, `peek()`, Topology Test Driver                   |

#### Join

| Module                                                                                                  | Library       | DSL                                                      | Additional Content                            |
|:--------------------------------------------------------------------------------------------------------|---------------|----------------------------------------------------------|-----------------------------------------------|
| [Join Stream-Global Table](/kafka-streams-quickstarts/kafka-streams-join-stream-global-table)           | Kafka Streams | `join()` between KStream and GlobalKTable                | `peek()`, Topology Test Driver                |
| [Join Stream-Stream](/kafka-streams-quickstarts/kafka-streams-join-stream-stream)                       | Kafka Streams | `join()` between KStream and KStream, `JoinWindows`      | `selectKey()`, `peek()`, Topology Test Driver |
| [Join Stream-Table](/kafka-streams-quickstarts/kafka-streams-join-stream-table)                         | Kafka Streams | `join()` between KStream and KTable                      | `selectKey()`, `peek()`, Topology Test Driver |
| [Left join Stream-Global Table](/kafka-streams-quickstarts/kafka-streams-left-join-stream-global-table) | Kafka Streams | `leftJoin()` between KStream and GlobalKTable            | `peek()`, Topology Test Driver                |
| [Left join Stream-Stream](/kafka-streams-quickstarts/kafka-streams-left-join-stream-stream)             | Kafka Streams | `leftJoin()` between KStream and KStream, `JoinWindows`  | `selectKey()`, `peek()`, Topology Test Driver |
| [Left join Stream-Table](/kafka-streams-quickstarts/kafka-streams-left-join-stream-table)               | Kafka Streams | `leftJoin()` between KStream and KTable                  | `selectKey()`, `peek()`, Topology Test Driver |
| [Outer join Stream-Stream](/kafka-streams-quickstarts/kafka-streams-outer-join-stream-stream)           | Kafka Streams | `outerJoin()` between KStream and KStream, `JoinWindows` | `selectKey()`, `peek()`, Topology Test Driver |

#### Windowing

| Module                                                                                | Library       | DSL                        |
|:--------------------------------------------------------------------------------------|---------------|----------------------------|
| [Tumbling Window](/kafka-streams-quickstarts/kafka-streams-aggregate-tumbling-window) | Kafka Streams | `windowedBy()`             |
| [Hopping Window](/kafka-streams-quickstarts/kafka-streams-aggregate-hopping-window)   | Kafka Streams | `windowedBy().advanceBy()` |
| [Sliding Window](/kafka-streams-quickstarts/kafka-streams-join-stream-stream)         | Kafka Streams | `JoinWindows`              |

#### Processor

| Module                                                                                    | Library       | DSL                                           | Additional Content                                                              |
|:------------------------------------------------------------------------------------------|---------------|-----------------------------------------------|---------------------------------------------------------------------------------|
| [Process](/kafka-streams-quickstarts/kafka-streams-process)                               | Kafka Streams | `process()`                                   | Headers, Topology Test Driver                                                   |
| [Process values](/kafka-streams-quickstarts/kafka-streams-process-values)                 | Kafka Streams | `processValues()`                             | Headers, Topology Test Driver                                                   |
| [Schedule](/kafka-streams-quickstarts/kafka-streams-schedule)                             | Kafka Streams | `process()`, `.schedule()`                    | Timestamped key-value store, Wall clock time, Stream time, Topology Test Driver |
| [Schedule Store Cleanup](/kafka-streams-quickstarts/kafka-streams-schedule-store-cleanup) | Kafka Streams | `process()`, `addStateStore()`, `.schedule()` | Key-value store, Stream time, Topology Test Driver                              |
