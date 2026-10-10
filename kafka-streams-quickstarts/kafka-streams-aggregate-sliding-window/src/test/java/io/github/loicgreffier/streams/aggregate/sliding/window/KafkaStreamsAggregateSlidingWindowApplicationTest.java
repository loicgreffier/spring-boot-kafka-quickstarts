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
package io.github.loicgreffier.streams.aggregate.sliding.window;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.StateStore.ORDER_AGGREGATE_SLIDING_WINDOW_STORE;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.ORDER_AGGREGATE_SLIDING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.ORDER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.OrderAggregate;
import io.github.loicgreffier.streams.aggregate.sliding.window.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.aggregate.sliding.window.serdes.SerdesUtils;
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

class KafkaStreamsAggregateSlidingWindowApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsAggregateSlidingWindowApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> inputTopic;
    private TestOutputTopic<String, OrderAggregate> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-aggregate-sliding-window-test");
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

        inputTopic = testDriver.createInputTopic(
                ORDER_TOPIC,
                new StringSerializer(),
                SerdesUtils.<Order>getValueSerdes().serializer());
        outputTopic = testDriver.createOutputTopic(
                ORDER_AGGREGATE_SLIDING_WINDOW_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<OrderAggregate>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Path.of(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldAggregateWhenTimeWindowIsRespected() {
        Order firstOrder = buildOrder(1L);
        inputTopic.pipeInput("1", firstOrder, Instant.parse("2000-01-01T01:00:00Z"));

        Order secondOrder = buildOrder(2L);
        inputTopic.pipeInput("2", secondOrder, Instant.parse("2000-01-01T01:02:00Z"));

        Order thirdOrder = buildOrder(3L);
        inputTopic.pipeInput("3", thirdOrder, Instant.parse("2000-01-01T01:04:00Z"));

        List<KeyValue<String, OrderAggregate>> results = outputTopic.readKeyValuesToList();

        // First order arrives
        assertEquals("1@2000-01-01T00:55:00Z->2000-01-01T01:00:00Z", results.getFirst().key);
        assertIterableEquals(List.of(firstOrder), results.getFirst().value.getOrders());

        // Second order arrives.
        assertEquals("1@2000-01-01T01:00:00.001Z->2000-01-01T01:05:00.001Z", results.get(1).key);
        assertIterableEquals(List.of(secondOrder), results.get(1).value.getOrders());

        assertEquals("1@2000-01-01T00:57:00Z->2000-01-01T01:02:00Z", results.get(2).key);
        assertIterableEquals(
                List.of(firstOrder, secondOrder), results.get(2).value.getOrders());

        // Third order arrives
        assertEquals("1@2000-01-01T01:00:00.001Z->2000-01-01T01:05:00.001Z", results.get(3).key);
        assertIterableEquals(
                List.of(secondOrder, thirdOrder), results.get(3).value.getOrders());

        assertEquals("1@2000-01-01T01:02:00.001Z->2000-01-01T01:07:00.001Z", results.get(4).key);
        assertIterableEquals(List.of(thirdOrder), results.get(4).value.getOrders());

        assertEquals("1@2000-01-01T00:59:00Z->2000-01-01T01:04:00Z", results.get(5).key);
        assertIterableEquals(
                List.of(firstOrder, secondOrder, thirdOrder),
                results.get(5).value.getOrders());

        WindowStore<String, OrderAggregate> stateStore =
                testDriver.getWindowStore(ORDER_AGGREGATE_SLIDING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, OrderAggregate> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, OrderAggregate> keyValue55To00 = iterator.next();
            assertEquals("1", keyValue55To00.key.key());
            assertEquals(
                    "2000-01-01T00:55:00Z",
                    keyValue55To00.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    keyValue55To00.key.window().endTime().toString());
            assertIterableEquals(List.of(firstOrder), keyValue55To00.value.getOrders());

            KeyValue<Windowed<String>, OrderAggregate> keyValue57To02 = iterator.next();
            assertEquals("1", keyValue57To02.key.key());
            assertEquals(
                    "2000-01-01T00:57:00Z",
                    keyValue57To02.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:02:00Z",
                    keyValue57To02.key.window().endTime().toString());
            assertIterableEquals(List.of(firstOrder, secondOrder), keyValue57To02.value.getOrders());

            KeyValue<Windowed<String>, OrderAggregate> keyValue59To04 = iterator.next();
            assertEquals("1", keyValue59To04.key.key());
            assertEquals(
                    "2000-01-01T00:59:00Z",
                    keyValue59To04.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:04:00Z",
                    keyValue59To04.key.window().endTime().toString());
            assertIterableEquals(List.of(firstOrder, secondOrder, thirdOrder), keyValue59To04.value.getOrders());

            KeyValue<Windowed<String>, OrderAggregate> keyValue00To05 = iterator.next();
            assertEquals("1", keyValue00To05.key.key());
            assertEquals(
                    "2000-01-01T01:00:00.001Z",
                    keyValue00To05.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:05:00.001Z",
                    keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of(secondOrder, thirdOrder), keyValue00To05.value.getOrders());

            KeyValue<Windowed<String>, OrderAggregate> keyValue02To07 = iterator.next();
            assertEquals("1", keyValue02To07.key.key());
            assertEquals(
                    "2000-01-01T01:02:00.001Z",
                    keyValue02To07.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:07:00.001Z",
                    keyValue02To07.key.window().endTime().toString());
            assertIterableEquals(List.of(thirdOrder), keyValue02To07.value.getOrders());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldNotAggregateWhenTimeWindowIsNotRespected() {
        Order firstOrder = buildOrder(1L);
        inputTopic.pipeInput("1", firstOrder, Instant.parse("2000-01-01T01:00:00Z"));

        Order secondOrder = buildOrder(2L);
        inputTopic.pipeInput("2", secondOrder, Instant.parse("2000-01-01T01:05:01Z"));

        List<KeyValue<String, OrderAggregate>> results = outputTopic.readKeyValuesToList();

        assertEquals("1@2000-01-01T00:55:00Z->2000-01-01T01:00:00Z", results.getFirst().key);
        assertIterableEquals(List.of(firstOrder), results.getFirst().value.getOrders());

        assertEquals("1@2000-01-01T01:00:01Z->2000-01-01T01:05:01Z", results.get(1).key);
        assertIterableEquals(List.of(secondOrder), results.get(1).value.getOrders());

        WindowStore<String, OrderAggregate> stateStore =
                testDriver.getWindowStore(ORDER_AGGREGATE_SLIDING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, OrderAggregate> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, OrderAggregate> keyValue00To05 = iterator.next();
            assertEquals("1", keyValue00To05.key.key());
            assertEquals(
                    "2000-01-01T00:55:00Z",
                    keyValue00To05.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of(firstOrder), keyValue00To05.value.getOrders());

            KeyValue<Windowed<String>, OrderAggregate> keyValue01To06 = iterator.next();
            assertEquals("1", keyValue01To06.key.key());
            assertEquals(
                    "2000-01-01T01:00:01Z",
                    keyValue01To06.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:05:01Z",
                    keyValue01To06.key.window().endTime().toString());
            assertIterableEquals(List.of(secondOrder), keyValue01To06.value.getOrders());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        Order firstOrder = buildOrder(1L);
        inputTopic.pipeInput("1", firstOrder, Instant.parse("2000-01-01T01:00:00Z"));

        Order secondOrder = buildOrder(2L);
        inputTopic.pipeInput("3", secondOrder, Instant.parse("2000-01-01T01:05:30Z"));

        // At this point, the stream time is 01:05:30. It exceeds by 30 seconds
        // the upper bound of the forward sliding window introduced by the first order
        // [01:00:00.001Z->01:05:00.001Z], where the third order should be included.
        // However, the following delayed third order will still be aggregated into this window
        // because the grace period is 1 minute.

        Order thirdOrder = buildOrder(3L);
        inputTopic.pipeInput("2", thirdOrder, Instant.parse("2000-01-01T01:03:00Z"));

        List<KeyValue<String, OrderAggregate>> results = outputTopic.readKeyValuesToList();

        // First order arrives
        assertEquals("1@2000-01-01T00:55:00Z->2000-01-01T01:00:00Z", results.getFirst().key);
        assertIterableEquals(List.of(firstOrder), results.getFirst().value.getOrders());

        // Second order arrives
        assertEquals("1@2000-01-01T01:00:30Z->2000-01-01T01:05:30Z", results.get(1).key);
        assertIterableEquals(List.of(secondOrder), results.get(1).value.getOrders());

        // Third order arrives
        assertEquals("1@2000-01-01T01:00:30Z->2000-01-01T01:05:30Z", results.get(2).key);
        assertIterableEquals(
                List.of(secondOrder, thirdOrder), results.get(2).value.getOrders());

        // Even if the stream time is 01:05:30, the window introduced by the first order
        // [01:00:00.001Z->01:05:00.001Z] is
        // not yet closed thanks to the grace period of 1 minute.
        // The third order whose timestamp is 01:03:00 is included in the window.
        assertEquals("1@2000-01-01T01:00:00.001Z->2000-01-01T01:05:00.001Z", results.get(3).key);
        assertIterableEquals(List.of(thirdOrder), results.get(3).value.getOrders());

        assertEquals("1@2000-01-01T01:03:00.001Z->2000-01-01T01:08:00.001Z", results.get(4).key);
        assertIterableEquals(
                List.of(secondOrder, thirdOrder), results.get(4).value.getOrders());

        WindowStore<String, OrderAggregate> stateStore =
                testDriver.getWindowStore(ORDER_AGGREGATE_SLIDING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, OrderAggregate> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, OrderAggregate> keyValue55To00 = iterator.next();
            assertEquals("1", keyValue55To00.key.key());
            assertEquals(
                    "2000-01-01T00:55:00Z",
                    keyValue55To00.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    keyValue55To00.key.window().endTime().toString());
            assertIterableEquals(List.of(firstOrder), keyValue55To00.value.getOrders());

            KeyValue<Windowed<String>, OrderAggregate> keyValue00To05 = iterator.next();
            assertEquals("1", keyValue00To05.key.key());
            assertEquals(
                    "2000-01-01T01:00:00.001Z",
                    keyValue00To05.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:05:00.001Z",
                    keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of(thirdOrder), keyValue00To05.value.getOrders());

            KeyValue<Windowed<String>, OrderAggregate> keyValue00m30To05m30 = iterator.next();
            assertEquals("1", keyValue00m30To05m30.key.key());
            assertEquals(
                    "2000-01-01T01:00:30Z",
                    keyValue00m30To05m30.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:05:30Z",
                    keyValue00m30To05m30.key.window().endTime().toString());
            assertIterableEquals(List.of(secondOrder, thirdOrder), keyValue00m30To05m30.value.getOrders());

            KeyValue<Windowed<String>, OrderAggregate> keyValue03To08 = iterator.next();
            assertEquals("1", keyValue03To08.key.key());
            assertEquals(
                    "2000-01-01T01:03:00.001Z",
                    keyValue03To08.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:08:00.001Z",
                    keyValue03To08.key.window().endTime().toString());
            assertIterableEquals(List.of(secondOrder, thirdOrder), keyValue03To08.value.getOrders());

            assertFalse(iterator.hasNext());
        }
    }

    private Order buildOrder(long id) {
        return Order.newBuilder()
                .setId(id)
                .setCustomerId(1L)
                .setItems(List.of("Laptop", "Mouse"))
                .setAmount(100.0)
                .build();
    }
}
