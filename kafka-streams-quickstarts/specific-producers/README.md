# Specific Producers

This module contains Kafka producers that provide the data required by the Kafka Streams applications.

- [Producer Customer](kafka-streams-producer-customer): produces the `<String,Customer>` customer referential, keyed by customer id, to the `CUSTOMER_TOPIC` topic.
- [Producer Order](kafka-streams-producer-order): produces `<String,Order>` records to the `ORDER_TOPIC` and `ORDER_TOPIC_TWO` topics, and `<String,Payment>` records to the `PAYMENT_TOPIC` topic. Orders and payments are keyed by order id, and one order out of five remains unpaid.
