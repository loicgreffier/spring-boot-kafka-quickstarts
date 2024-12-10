<div align="center">

<img src=".readme/kafka.png" alt="Apache Kafka"/>
<img src=".readme/spring-boot.png" alt="Spring Boot"/>

# Spring Boot and Kafka quickstarts

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/loicgreffier/spring-boot-kafka-quickstarts/push_main.yml?branch=main&logo=github&style=for-the-badge)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts/actions/workflows/push_main.yml)
[![Kafka Version](https://img.shields.io/badge/dynamic/xml?url=https%3A%2F%2Fraw.githubusercontent.com%2Floicgreffier%2Fspring-boot-kafka-quickstarts%2Fmain%2Fpom.xml&query=%2F*%5Blocal-name()%3D'project'%5D%2F*%5Blocal-name()%3D'properties'%5D%2F*%5Blocal-name()%3D'kafka.version'%5D%2Ftext()&style=for-the-badge&logo=apachekafka&label=version)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts/blob/main/pom.xml)
[![Spring Boot Version](https://img.shields.io/badge/dynamic/xml?url=https%3A%2F%2Fraw.githubusercontent.com%2Floicgreffier%2Fspring-boot-kafka-quickstarts%2Fmain%2Fpom.xml&query=%2F*%5Blocal-name()%3D'project'%5D%2F*%5Blocal-name()%3D'parent'%5D%2F*%5Blocal-name()%3D'version'%5D%2Ftext()&style=for-the-badge&logo=spring-boot&label=version)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts/blob/main/pom.xml)
[![GitHub Stars](https://img.shields.io/github/stars/loicgreffier/spring-boot-kafka-quickstarts?logo=github&style=for-the-badge)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts)
[![GitHub Watch](https://img.shields.io/github/watchers/loicgreffier/spring-boot-kafka-quickstarts?logo=github&style=for-the-badge)](https://github.com/loicgreffier/spring-boot-kafka-quickstarts)
[![Docker Pulls](https://img.shields.io/docker/pulls/loicgreffier/spring-boot-kafka-quickstarts?label=Pulls&logo=docker&style=for-the-badge)](https://hub.docker.com/r/loicgreffier/spring-boot-kafka-quickstarts/tags)
[![Docker Stars](https://img.shields.io/docker/stars/loicgreffier/spring-boot-kafka-quickstarts?label=Stars&logo=docker&style=for-the-badge)](https://hub.docker.com/r/loicgreffier/spring-boot-kafka-quickstarts)

[Producer](#producers) • [Consumer](#consumers) • [Streams](#kafka-streams)

Code samples around Kafka Clients and Kafka Streams leveraging Spring Boot to simplify the development of applications.

</div>

## Requirements

- Java 21
- Maven
- Docker

## Quickstarts list

### Producers

| Module                                                                    | Library       | Content                                                 |
|---------------------------------------------------------------------------|---------------|---------------------------------------------------------|
| [Avro Generic](/kafka-producer-quickstarts/kafka-producer-avro-generic)   | Kafka Clients | Produce generic Avro records                            |
| [Avro Specific](/kafka-producer-quickstarts/kafka-producer-avro-specific) | Kafka Clients | Produce specific Avro records                           |
| [Headers](/kafka-producer-quickstarts/kafka-producer-headers)             | Kafka Clients | Produce records with headers                            |
| [Simple](/kafka-producer-quickstarts/kafka-producer-simple)               | Kafka Clients | Produce String records                                  |
| [Transaction](/kafka-producer-quickstarts/kafka-producer-transaction)     | Kafka Clients | Produce to multiple topics while guaranteeing atomicity |

### Consumers

| Module                                                                                    | Library       | Content                                                        |
|:------------------------------------------------------------------------------------------|---------------|----------------------------------------------------------------|
| [Avro Generic](/kafka-consumer-quickstarts/kafka-consumer-avro-generic)                   | Kafka Clients | Consume generic Avro records                                   |
| [Avro Specific](/kafka-consumer-quickstarts/kafka-consumer-avro-specific)                 | Kafka Clients | Consume specific Avro records                                  |
| [Circuit breaker](/kafka-consumer-quickstarts/kafka-consumer-circuit-breaker)             | Kafka Clients | Consume records while handling poison pills                    |
| [Headers](/kafka-consumer-quickstarts/kafka-consumer-headers)                             | Kafka Clients | Consume records with headers                                   |
| [Retry external system](/kafka-consumer-quickstarts/kafka-consumer-retry-external-system) | Kafka Clients | Consume records while retrying on failed external system calls |
| [Simple](/kafka-consumer-quickstarts/kafka-consumer-simple)                               | Kafka Clients | Consume String records                                         |
| [Transaction](/kafka-consumer-quickstarts/kafka-consumer-transaction)                     | Kafka Clients | Consume records from committed transactions                    |

### Kafka Streams

#### Source

| Module                                                                            | Library       |                              | DSL             |
|:----------------------------------------------------------------------------------|---------------|------------------------------|-----------------|
| [Global Table](/kafka-streams-quickstarts/kafka-streams-join-stream-global-table) | Kafka Streams | Source topic as global table | `globalTable()` |
| [Table](/kafka-streams-quickstarts/kafka-streams-join-stream-table)               | Kafka Streams | Source topic as table        | `table()`       |
| [Stream](/kafka-streams-quickstarts/kafka-streams-map)                            | Kafka Streams | Source topic as stream       | `stream()`      |

#### Stateless

| Module                                                                   | Library       | Content                                                | DSL                       |
|:-------------------------------------------------------------------------|---------------|--------------------------------------------------------|---------------------------|
| [Branch](/kafka-streams-quickstarts/kafka-streams-branch)                | Kafka Streams | Split and create branches from a stream                | `split()`, `branch()`     |
| [Cogroup](/kafka-streams-quickstarts/kafka-streams-cogroup)              | Kafka Streams | Aggregate records of multiple streams by key           | `cogroup()`               |
| [Filter](/kafka-streams-quickstarts/kafka-streams-filter)                | Kafka Streams | Retain or drop records based on a predicate            | `filter()`, `filterNot()` |
| [FlatMap](/kafka-streams-quickstarts/kafka-streams-flatmap)              | Kafka Streams | Change one record into 0, 1 or _n_ records             | `flatMap()`               |
| [FlatMapValues](/kafka-streams-quickstarts/kafka-streams-flatmap-values) | Kafka Streams | Change one record value into 0, 1 or _n_ record values | `flatMapValues()`         |
| [Foreach](/kafka-streams-quickstarts/kafka-streams-foreach)              | Kafka Streams | Perform a terminal operation on each record            | `foreach()`               |
| [Map](/kafka-streams-quickstarts/kafka-streams-map)                      | Kafka Streams | Change one record into another record                  | `map()`                   |
| [MapValues](/kafka-streams-quickstarts/kafka-streams-map-values)         | Kafka Streams | Change one record value into another record value      | `mapValues()`             |
| [Merge](/kafka-streams-quickstarts/kafka-streams-merge)                  | Kafka Streams | Merge two streams into one stream                      | `merge()`                 |
| [Print](/kafka-streams-quickstarts/kafka-streams-print)                  | Kafka Streams | Print a stream to the system output or a file          | `print()`                 |
| [Repartition](/kafka-streams-quickstarts/kafka-streams-repartition)      | Kafka Streams | Trigger a repartitioning of the stream                 | `repartition()`           |
| [SelectKey](/kafka-streams-quickstarts/kafka-streams-select-key)         | Kafka Streams | Change the key of each record                          | `selectKey()`             |

#### Aggregate

| Module                                                                                          | Library       | Content                                                              | DSL                                                          |
|:------------------------------------------------------------------------------------------------|---------------|----------------------------------------------------------------------|--------------------------------------------------------------|
| [Aggregate](/kafka-streams-quickstarts/kafka-streams-aggregate)                                 | Kafka Streams | Aggregate a stream by key in a single object                         | `groupByKey()`, `aggregate()`                                |
| [Aggregate Hopping Window](/kafka-streams-quickstarts/kafka-streams-aggregate-hopping-window)   | Kafka Streams | Aggregate a stream by key and by hopping window with a grace period  | `groupByKey()`, `aggregate()`, `windowedBy()`, `advanceBy()` |
| [Aggregate Sliding Window](/kafka-streams-quickstarts/kafka-streams-aggregate-sliding-window)   | Kafka Streams | Aggregate a stream by key and by sliding window with a grace period  | `groupByKey()`, `aggregate()`, `windowedBy()`                |
| [Aggregate Tumbling Window](/kafka-streams-quickstarts/kafka-streams-aggregate-tumbling-window) | Kafka Streams | Aggregate a stream by key and by tumbling window with a grace period | `groupByKey()`, `aggregate()`, `windowedBy()`                |
| [Average](/kafka-streams-quickstarts/kafka-streams-average)                                     | Kafka Streams | Compute an average value of a stream by key                          | `groupBy()`, `aggregate()`                                   |
| [Count](/kafka-streams-quickstarts/kafka-streams-count)                                         | Kafka Streams | Count the number of records of a stream by key                       | `groupBy()`, `count()`                                       |
| [Reduce](/kafka-streams-quickstarts/kafka-streams-reduce)                                       | Kafka Streams | Reduce the records of a stream by key                                | `groupBy()`, `reduce()`                                      |

#### Join

| Module                                                                                                  | Library       | Content                                                   | DSL                  |
|:--------------------------------------------------------------------------------------------------------|---------------|-----------------------------------------------------------|----------------------|
| [Join Stream-Global Table](/kafka-streams-quickstarts/kafka-streams-join-stream-global-table)           | Kafka Streams | Perform an inner join between a stream and a global table | `join()`             |
| [Join Stream-Stream](/kafka-streams-quickstarts/kafka-streams-join-stream-stream)                       | Kafka Streams | Perform an inner join between two streams                 | `join()`             |
| [Join Stream-Table](/kafka-streams-quickstarts/kafka-streams-join-stream-table)                         | Kafka Streams | Perform an inner join between a stream and a table        | `join()`             |
| [Left Join Stream-Global Table](/kafka-streams-quickstarts/kafka-streams-left-join-stream-global-table) | Kafka Streams | Perform a left join between a stream and a global table   | `leftJoin()`         |
| [Left Join Stream-Stream](/kafka-streams-quickstarts/kafka-streams-left-join-stream-stream)             | Kafka Streams | Perform a left join between two streams                   | `leftJoin()`         |
| [Left Join Stream-Table](/kafka-streams-quickstarts/kafka-streams-left-join-stream-table)               | Kafka Streams | Perform a left join between a stream and a table          | `leftJoin()`         |
| [Outer Join Stream-Stream](/kafka-streams-quickstarts/kafka-streams-outer-join-stream-stream)           | Kafka Streams | Perform an outer join between two streams                 | `outerJoin()`        |

#### Windowing

| Module                                                                                | Library       | Content                                              | DSL                           |
|:--------------------------------------------------------------------------------------|---------------|------------------------------------------------------|-------------------------------|
| [Hopping Window](/kafka-streams-quickstarts/kafka-streams-aggregate-hopping-window)   | Kafka Streams | Group records by hopping window with a grace period  | `windowedBy()`, `advanceBy()` |
| [Sliding Window](/kafka-streams-quickstarts/kafka-streams-aggregate-sliding-window)   | Kafka Streams | Group records by sliding window with a grace period  | `windowedBy()`                |
| [Tumbling Window](/kafka-streams-quickstarts/kafka-streams-aggregate-tumbling-window) | Kafka Streams | Group records by tumbling window with a grace period | `windowedBy()`                |

#### Processor

| Module                                                                           | Library       | Content                                                                 | DSL               | Processor API                                |
|:---------------------------------------------------------------------------------|---------------|-------------------------------------------------------------------------|-------------------|----------------------------------------------|
| [Process](/kafka-streams-quickstarts/kafka-streams-process)                      | Kafka Streams | Apply a processor to a stream                                           | `process()`       | `context()`, `forward()`, `Record#headers()` |
| [ProcessValues](/kafka-streams-quickstarts/kafka-streams-process-values)         | Kafka Streams | Apply a fixed key processor to a stream                                 | `processValues()` | `context()`, `forward()`, `Record#headers()` |
| [Schedule](/kafka-streams-quickstarts/kafka-streams-schedule)                    | Kafka Streams | Schedule punctuation functions based on wall clock time and stream time | `process()`       | `schedule()`, `getStateStore()`              |

#### Stores

| Module                                                                                              | Library       | Content                                                        | DSL                            | Processor API                   |
|:----------------------------------------------------------------------------------------------------|---------------|----------------------------------------------------------------|--------------------------------|---------------------------------|
| [Store Cleanup](/kafka-streams-quickstarts/kafka-streams-store-cleanup)                             | Kafka Streams | Schedule periodic store cleanup based on stream time           | `process()`                    | `schedule()`, `getStateStore()` |
| [Key-Value Store](/kafka-streams-quickstarts/kafka-streams-store-key-value)                         | Kafka Streams | Insert and retrieve records from a key-value store             | `process()`, `addStateStore()` | `getStateStore()`               |
| [Timestamped Key-Value Store](/kafka-streams-quickstarts/kafka-streams-store-timestamped-key-value) | Kafka Streams | Insert and retrieve records from a timestamped key-value store | `process()`, `addStateStore()` | `getStateStore()`               |

#### Handler

| Module                                                                                                          | Library       | Content                                        | Config                                      |
|:----------------------------------------------------------------------------------------------------------------|---------------|------------------------------------------------|---------------------------------------------|
| [Deserialization Exception Handler](/kafka-streams-quickstarts/kafka-streams-exception-handler-deserialization) | Kafka Streams | Handle deserialization exceptions              | `default.deserialization.exception.handler` |
| [Processing Exception Handler](/kafka-streams-quickstarts/kafka-streams-exception-handler-processing)           | Kafka Streams | Handle processing exceptions                   | `processing.exception.handler`              |
| [Production Exception Handler](/kafka-streams-quickstarts/kafka-streams-exception-handler-production)           | Kafka Streams | Handle production and serialization exceptions | `default.production.exception.handler`      |

## Credits

[Confluent - Crossing the Streams](https://www.confluent.io/blog/crossing-streams-joins-apache-kafka)

[Bill Bejeck - Windowing time semantics](https://www.linkedin.com/pulse/mastering-stream-processing-windowing-time-semantics-bill-bejeck-fp50e)

[Apache Kafka - Sliding Windows](https://kafka.apache.org/30/javadoc/org/apache/kafka/streams/kstream/SlidingWindows.html)

[Jeanlouisboudart - Retriable Consumer](https://github.com/jeanlouisboudart/retriable-consumer)
