/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.github.loicgreffier.streams.join.stream.stream;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.join.stream.stream.constant.StateStore.ORDER_DELIVERY_JOIN_STREAM_STREAM_STORE;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.DELIVERY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.ORDER_DELIVERY_JOIN_STREAM_STREAM_REKEY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.ORDER_DELIVERY_JOIN_STREAM_STREAM_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.ORDER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.Delivery;
import io.github.loicgreffier.avro.JoinOrderDelivery;
import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.streams.join.stream.stream.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.join.stream.stream.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsJoinStreamStreamApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsJoinStreamStreamApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> orderInputTopic;
    private TestInputTopic<String, Delivery> deliveryInputTopic;
    private TestOutputTopic<String, Delivery> rekeyDeliveryOutputTopic;
    private TestOutputTopic<String, JoinOrderDelivery> joinOutputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-join-stream-stream-test");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create SerDes
        Map<String, String> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        SerdesUtils.setSerdesConfig(config);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00Z"));

        orderInputTopic = testDriver.createInputTopic(
                ORDER_TOPIC,
                new StringSerializer(),
                SerdesUtils.<Order>getValueSerdes().serializer());
        deliveryInputTopic = testDriver.createInputTopic(
                DELIVERY_TOPIC,
                new StringSerializer(),
                SerdesUtils.<Delivery>getValueSerdes().serializer());
        rekeyDeliveryOutputTopic = testDriver.createOutputTopic(
                "streams-join-stream-stream-test-" + ORDER_DELIVERY_JOIN_STREAM_STREAM_REKEY_TOPIC
                        + "-right-repartition",
                new StringDeserializer(),
                SerdesUtils.<Delivery>getValueSerdes().deserializer());
        joinOutputTopic = testDriver.createOutputTopic(
                ORDER_DELIVERY_JOIN_STREAM_STREAM_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<JoinOrderDelivery>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Path.of(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldRekey() {
        Delivery delivery = buildDelivery("DEL10", "ORD1");
        deliveryInputTopic.pipeInput("DEL10", delivery);

        List<KeyValue<String, Delivery>> results = rekeyDeliveryOutputTopic.readKeyValuesToList();

        // Only the delivery stream is rekeyed, as the order stream is already keyed by order id.
        assertEquals(KeyValue.pair("ORD1", delivery), results.getFirst());
    }

    @Test
    void shouldJoinWhenTimeWindowIsRespected() {
        Order orderOne = buildOrder("ORD1");
        orderInputTopic.pipeInput("ORD1", orderOne, Instant.parse("2000-01-01T01:00:00Z"));

        Delivery deliveryOne = buildDelivery("DEL1", "ORD1");
        deliveryInputTopic.pipeInput("DEL1", deliveryOne, Instant.parse("2000-01-01T01:02:00Z"));

        Order orderTwo = buildOrder("ORD2");
        orderInputTopic.pipeInput("ORD2", orderTwo, Instant.parse("2000-01-01T01:03:00Z"));

        Delivery deliveryTwo = buildDelivery("DEL2", "ORD2");
        deliveryInputTopic.pipeInput("DEL2", deliveryTwo, Instant.parse("2000-01-01T01:04:00Z"));

        List<KeyValue<String, JoinOrderDelivery>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("ORD1", results.getFirst().key);
        assertEquals(orderOne, results.getFirst().value.getOrder());
        assertEquals(deliveryOne, results.getFirst().value.getDelivery());

        assertEquals("ORD2", results.get(1).key);
        assertEquals(orderTwo, results.get(1).value.getOrder());
        assertEquals(deliveryTwo, results.get(1).value.getDelivery());

        // As join windows are looking backward and forward in time,
        // records are kept in the store for "before" + "after" duration.
        WindowStore<String, Order> orderStore =
                testDriver.getWindowStore(ORDER_DELIVERY_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, Order> iterator = orderStore.all()) {
            assertWindowedRecord(iterator.next(), "ORD1", "2000-01-01T01:00:00Z", "2000-01-01T01:10:00Z", orderOne);
            assertWindowedRecord(iterator.next(), "ORD2", "2000-01-01T01:03:00Z", "2000-01-01T01:13:00Z", orderTwo);
            assertFalse(iterator.hasNext());
        }

        WindowStore<String, Delivery> deliveryStore =
                testDriver.getWindowStore(ORDER_DELIVERY_JOIN_STREAM_STREAM_STORE + "-other-join-store");

        try (KeyValueIterator<Windowed<String>, Delivery> iterator = deliveryStore.all()) {
            assertWindowedRecord(iterator.next(), "ORD1", "2000-01-01T01:02:00Z", "2000-01-01T01:12:00Z", deliveryOne);
            assertWindowedRecord(iterator.next(), "ORD2", "2000-01-01T01:04:00Z", "2000-01-01T01:14:00Z", deliveryTwo);
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldNotJoinWhenTimeWindowIsNotRespected() {
        Order order = buildOrder("ORD1");
        orderInputTopic.pipeInput("ORD1", order, Instant.parse("2000-01-01T01:00:00Z"));

        Delivery delivery = buildDelivery("DEL1", "ORD1");
        deliveryInputTopic.pipeInput("DEL1", delivery, Instant.parse("2000-01-01T01:05:01Z"));

        List<KeyValue<String, JoinOrderDelivery>> results = joinOutputTopic.readKeyValuesToList();

        // No records joined because the delivery arrived too late for the order.
        assertTrue(results.isEmpty());

        WindowStore<String, Order> orderStore =
                testDriver.getWindowStore(ORDER_DELIVERY_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, Order> iterator = orderStore.all()) {
            assertWindowedRecord(iterator.next(), "ORD1", "2000-01-01T01:00:00Z", "2000-01-01T01:10:00Z", order);
            assertFalse(iterator.hasNext());
        }

        WindowStore<String, Delivery> deliveryStore =
                testDriver.getWindowStore(ORDER_DELIVERY_JOIN_STREAM_STREAM_STORE + "-other-join-store");

        try (KeyValueIterator<Windowed<String>, Delivery> iterator = deliveryStore.all()) {
            assertWindowedRecord(iterator.next(), "ORD1", "2000-01-01T01:05:01Z", "2000-01-01T01:15:01Z", delivery);
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        Order orderOne = buildOrder("ORD1");
        orderInputTopic.pipeInput("ORD1", orderOne, Instant.parse("2000-01-01T01:00:00Z"));

        Order orderTwo = buildOrder("ORD2");
        orderInputTopic.pipeInput("ORD2", orderTwo, Instant.parse("2000-01-01T01:10:30Z"));

        // At this point, the stream time is 01:10:30. It exceeds by 30 seconds
        // the upper bound of the first order's window [01:00:00.001Z->01:10:00Z] in the store.
        // However, the following delayed delivery will be joined with the first order
        // thanks to the grace period of 1 minute.

        Delivery delivery = buildDelivery("DEL1", "ORD1");
        deliveryInputTopic.pipeInput("DEL1", delivery, Instant.parse("2000-01-01T01:05:00Z"));

        List<KeyValue<String, JoinOrderDelivery>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("ORD1", results.getFirst().key);
        assertEquals(orderOne, results.getFirst().value.getOrder());
        assertEquals(delivery, results.getFirst().value.getDelivery());

        WindowStore<String, Order> orderStore =
                testDriver.getWindowStore(ORDER_DELIVERY_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, Order> iterator = orderStore.all()) {
            assertWindowedRecord(iterator.next(), "ORD1", "2000-01-01T01:00:00Z", "2000-01-01T01:10:00Z", orderOne);
            assertWindowedRecord(iterator.next(), "ORD2", "2000-01-01T01:10:30Z", "2000-01-01T01:20:30Z", orderTwo);
            assertFalse(iterator.hasNext());
        }

        WindowStore<String, Delivery> deliveryStore =
                testDriver.getWindowStore(ORDER_DELIVERY_JOIN_STREAM_STREAM_STORE + "-other-join-store");

        try (KeyValueIterator<Windowed<String>, Delivery> iterator = deliveryStore.all()) {
            assertWindowedRecord(iterator.next(), "ORD1", "2000-01-01T01:05:00Z", "2000-01-01T01:15:00Z", delivery);
            assertFalse(iterator.hasNext());
        }
    }

    private <T> void assertWindowedRecord(
            KeyValue<Windowed<String>, T> keyValue, String key, String start, String end, T value) {
        assertEquals(key, keyValue.key.key());
        assertEquals(start, keyValue.key.window().startTime().toString());
        assertEquals(end, keyValue.key.window().endTime().toString());
        assertEquals(value, keyValue.value);
    }

    private Order buildOrder(String id) {
        return Order.newBuilder()
                .setId(id)
                .setCustomerId("CUS1")
                .setItems(List.of("Laptop", "Mouse"))
                .setAmount(1249.90)
                .build();
    }

    private Delivery buildDelivery(String id, String orderId) {
        return Delivery.newBuilder()
                .setId(id)
                .setOrderId(orderId)
                .setDestination("Lyon")
                .build();
    }
}
