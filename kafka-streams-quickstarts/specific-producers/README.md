# Specific Producers

This module contains Kafka producers that provide the data required by the Kafka Streams applications.

- [Producer Customer](kafka-streams-producer-customer): produces the `<String,Customer>` customer referential, keyed by customer id, to the `CUSTOMER_TOPIC` topic.
- [Producer Delivery](kafka-streams-producer-delivery): produces `<String,Delivery>` records, keyed by order id, to the `DELIVERY_TOPIC` topic. Delivery i ships order i, so every order is delivered.
- [Producer Order](kafka-streams-producer-order): produces `<String,Order>` records, keyed by order id, to the `ORDER_TOPIC` and `ORDER_TOPIC_TWO` topics.
