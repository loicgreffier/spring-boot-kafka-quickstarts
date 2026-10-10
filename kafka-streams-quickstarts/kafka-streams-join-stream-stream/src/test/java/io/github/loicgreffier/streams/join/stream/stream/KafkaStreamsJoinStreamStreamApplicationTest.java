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
import static io.github.loicgreffier.streams.join.stream.stream.constant.StateStore.ORDER_PAYMENT_JOIN_STREAM_STREAM_STORE;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.ORDER_PAYMENT_JOIN_STREAM_STREAM_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.ORDER_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.PAYMENT_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.JoinOrderPayment;
import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.Payment;
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
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsJoinStreamStreamApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsJoinStreamStreamApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> orderInputTopic;
    private TestInputTopic<String, Payment> paymentInputTopic;
    private TestOutputTopic<String, JoinOrderPayment> joinOutputTopic;

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
        paymentInputTopic = testDriver.createInputTopic(
                PAYMENT_TOPIC,
                new StringSerializer(),
                SerdesUtils.<Payment>getValueSerdes().serializer());
        joinOutputTopic = testDriver.createOutputTopic(
                ORDER_PAYMENT_JOIN_STREAM_STREAM_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<JoinOrderPayment>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Path.of(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldJoinWhenTimeWindowIsRespected() {
        Order orderOne = buildOrder(1L);
        orderInputTopic.pipeInput(new TestRecord<>("1", orderOne, Instant.parse("2000-01-01T01:00:00Z")));

        Payment paymentOne = buildPayment(1L);
        paymentInputTopic.pipeInput(new TestRecord<>("1", paymentOne, Instant.parse("2000-01-01T01:02:00Z")));

        Order orderTwo = buildOrder(2L);
        orderInputTopic.pipeInput(new TestRecord<>("2", orderTwo, Instant.parse("2000-01-01T01:03:00Z")));

        Payment paymentTwo = buildPayment(2L);
        paymentInputTopic.pipeInput(new TestRecord<>("2", paymentTwo, Instant.parse("2000-01-01T01:04:00Z")));

        List<KeyValue<String, JoinOrderPayment>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("1", results.getFirst().key);
        assertEquals(orderOne, results.getFirst().value.getOrder());
        assertEquals(paymentOne, results.getFirst().value.getPayment());

        assertEquals("2", results.get(1).key);
        assertEquals(orderTwo, results.get(1).value.getOrder());
        assertEquals(paymentTwo, results.get(1).value.getPayment());

        // As join windows are looking backward and forward in time,
        // records are kept in the store for "before" + "after" duration.
        WindowStore<String, Order> orderStateStore =
                testDriver.getWindowStore(ORDER_PAYMENT_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, Order> iterator = orderStateStore.all()) {
            assertWindowedRecord(iterator.next(), "1", "2000-01-01T01:00:00Z", "2000-01-01T01:10:00Z", orderOne);
            assertWindowedRecord(iterator.next(), "2", "2000-01-01T01:03:00Z", "2000-01-01T01:13:00Z", orderTwo);
            assertFalse(iterator.hasNext());
        }

        WindowStore<String, Payment> paymentStateStore =
                testDriver.getWindowStore(ORDER_PAYMENT_JOIN_STREAM_STREAM_STORE + "-other-join-store");

        try (KeyValueIterator<Windowed<String>, Payment> iterator = paymentStateStore.all()) {
            assertWindowedRecord(iterator.next(), "1", "2000-01-01T01:02:00Z", "2000-01-01T01:12:00Z", paymentOne);
            assertWindowedRecord(iterator.next(), "2", "2000-01-01T01:04:00Z", "2000-01-01T01:14:00Z", paymentTwo);
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldNotJoinWhenTimeWindowIsNotRespected() {
        Order order = buildOrder(1L);
        orderInputTopic.pipeInput(new TestRecord<>("1", order, Instant.parse("2000-01-01T01:00:00Z")));

        Payment payment = buildPayment(1L);
        paymentInputTopic.pipeInput(new TestRecord<>("1", payment, Instant.parse("2000-01-01T01:05:01Z")));

        List<KeyValue<String, JoinOrderPayment>> results = joinOutputTopic.readKeyValuesToList();

        // No records joined because the payment arrived too late for the order.
        assertTrue(results.isEmpty());

        WindowStore<String, Order> orderStateStore =
                testDriver.getWindowStore(ORDER_PAYMENT_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, Order> iterator = orderStateStore.all()) {
            assertWindowedRecord(iterator.next(), "1", "2000-01-01T01:00:00Z", "2000-01-01T01:10:00Z", order);
            assertFalse(iterator.hasNext());
        }

        WindowStore<String, Payment> paymentStateStore =
                testDriver.getWindowStore(ORDER_PAYMENT_JOIN_STREAM_STREAM_STORE + "-other-join-store");

        try (KeyValueIterator<Windowed<String>, Payment> iterator = paymentStateStore.all()) {
            assertWindowedRecord(iterator.next(), "1", "2000-01-01T01:05:01Z", "2000-01-01T01:15:01Z", payment);
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        Order orderOne = buildOrder(1L);
        orderInputTopic.pipeInput(new TestRecord<>("1", orderOne, Instant.parse("2000-01-01T01:00:00Z")));

        Order orderTwo = buildOrder(2L);
        orderInputTopic.pipeInput(new TestRecord<>("2", orderTwo, Instant.parse("2000-01-01T01:10:30Z")));

        // At this point, the stream time is 01:10:30. It exceeds by 30 seconds
        // the upper bound of the first order's window [01:00:00.001Z->01:10:00Z] in the store.
        // However, the following delayed payment will be joined with the first order
        // thanks to the grace period of 1 minute.

        Payment payment = buildPayment(1L);
        paymentInputTopic.pipeInput(new TestRecord<>("1", payment, Instant.parse("2000-01-01T01:05:00Z")));

        List<KeyValue<String, JoinOrderPayment>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("1", results.getFirst().key);
        assertEquals(orderOne, results.getFirst().value.getOrder());
        assertEquals(payment, results.getFirst().value.getPayment());

        WindowStore<String, Order> orderStateStore =
                testDriver.getWindowStore(ORDER_PAYMENT_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, Order> iterator = orderStateStore.all()) {
            assertWindowedRecord(iterator.next(), "1", "2000-01-01T01:00:00Z", "2000-01-01T01:10:00Z", orderOne);
            assertWindowedRecord(iterator.next(), "2", "2000-01-01T01:10:30Z", "2000-01-01T01:20:30Z", orderTwo);
            assertFalse(iterator.hasNext());
        }

        WindowStore<String, Payment> paymentStateStore =
                testDriver.getWindowStore(ORDER_PAYMENT_JOIN_STREAM_STREAM_STORE + "-other-join-store");

        try (KeyValueIterator<Windowed<String>, Payment> iterator = paymentStateStore.all()) {
            assertWindowedRecord(iterator.next(), "1", "2000-01-01T01:05:00Z", "2000-01-01T01:15:00Z", payment);
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

    private Order buildOrder(Long id) {
        return Order.newBuilder()
                .setId(id)
                .setCustomerId(3L)
                .setItems(List.of("Laptop", "Mouse"))
                .setAmount(1249.90)
                .build();
    }

    private Payment buildPayment(Long orderId) {
        return Payment.newBuilder()
                .setId(orderId)
                .setOrderId(orderId)
                .setAmount(1249.90)
                .build();
    }
}
