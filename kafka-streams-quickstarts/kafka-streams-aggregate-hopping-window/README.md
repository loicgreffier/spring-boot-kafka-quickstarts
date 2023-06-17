# Kafka Streams Aggregate Hopping Window

This module demonstrates the following:

- The usage of the Kafka Streams DSL, including `aggregate()`, `windowedBy().advanceBy()`, `groupByKey()`, `selectKey()`, `toStream()` and `peek()`.
- Unit testing using the Topology Test Driver.

In this module, records of type `<String, KafkaPerson>` are streamed from a topic named `PERSON_TOPIC`. 
The following tasks are performed:

1. Group the stream by last name using `groupByKey()` operation.
2. Apply an aggregator that combines each `KafkaPerson` record with the same last name into a `KafkaPersonGroup` object and aggregates the first names by last name.
3. The aggregations are performed using a time window of 5 minutes and a 2-minute hop. This means that the aggregation is updated every 2 minutes based on a 5-minute window of data.
4. Write the resulting records to a new topic named `PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC`.

The output records will be in the following format:

```json
{"firstNameByLastName":{"Last name 1":{"First name 1", "First name 2", "First name 3"}}}
{"firstNameByLastName":{"Last name 2":{"First name 4", "First name 5", "First name 6"}}}
{"firstNameByLastName":{"Last name 3":{"First name 7", "First name 8", "First name 9"}}}
```

![topology.png](topology.png)

## Requirements

To compile and run this demo, you will need the following:

- Java 17
- Maven
- Docker

## Running the Application

To run the application manually, please follow the steps below:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce records of type `<String, KafkaPerson>` to a topic named `PERSON_TOPIC`. You can use the [producer person](../specific-producers/kafka-streams-producer-person) to do this.
- Start the Kafka Streams.

To run the application in Docker, please use the following command:

```console
docker-compose up -d
```

This command will start the following services in Docker:

- 1 Zookeeper
- 1 Kafka broker
- 1 Schema registry
- 1 Control Center
- 1 producer person
- 1 Kafka Streams aggregate hopping window
