# Consumer Transaction

This module consumes orders and payments serialized as JSON strings, of type `<String, String>`, from two topics: `ORDER_JSON_TOPIC` and `PAYMENT_JSON_TOPIC`.

It demonstrates the following:

- How to use the Kafka Clients consumer API.
- How to configure the consumer's `isolation.level` to `read_committed`, ensuring that only committed records are consumed while filtering out uncommitted or aborted transactional records.
- How to write unit tests with a mock consumer.

## Prerequisites

To compile and run this demo, you'll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/get-started/platform-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Produce orders and payments of type `<String, String>` to the `ORDER_JSON_TOPIC` and `PAYMENT_JSON_TOPIC`. You can use the [Producer Transaction](../../kafka-producer-quickstarts/kafka-producer-transaction) for this.
- Start the consumer.

Alternatively, to run everything at once using Docker, run:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Control Center
- Producer Transaction
- Consumer Transaction
