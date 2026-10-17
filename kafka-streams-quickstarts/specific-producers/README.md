# Specific Producers

This module contains Kafka producers that provide the data required by the Kafka Streams applications.

- [Producer Customer](kafka-streams-producer-customer): produces `<String, Customer>` records to the `CUSTOMER_TOPIC`.
- [Producer Delivery](kafka-streams-producer-delivery): produces `<String, Delivery>` records to the `DELIVERY_TOPIC`.
- [Producer Order](kafka-streams-producer-order): produces `<String, Order>` records to the `ORDER_TOPIC` and `ORDER_TOPIC_TWO`.
