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
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.StateStore.USER_AGGREGATE_SLIDING_WINDOW_STORE;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.USER_AGGREGATE_SLIDING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.USER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.avro.KafkaUserAggregate;
import io.github.loicgreffier.streams.aggregate.sliding.window.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.aggregate.sliding.window.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

class KafkaStreamsAggregateSlidingWindowApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsAggregateSlidingWindowApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaUser> inputTopic;
    private TestOutputTopic<String, KafkaUserAggregate> outputTopic;

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
                USER_TOPIC,
                new StringSerializer(),
                SerdesUtils.<KafkaUser>getValueSerdes().serializer());
        outputTopic = testDriver.createOutputTopic(
                USER_AGGREGATE_SLIDING_WINDOW_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<KafkaUserAggregate>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldAggregateWhenTimeWindowIsRespected() {
        KafkaUser homer = buildKafkaUser("Homer");
        inputTopic.pipeInput("1", homer, Instant.parse("2000-01-01T01:00:00Z"));

        KafkaUser marge = buildKafkaUser("Marge");
        inputTopic.pipeInput("2", marge, Instant.parse("2000-01-01T01:02:00Z"));

        KafkaUser bart = buildKafkaUser("Bart");
        inputTopic.pipeInput("3", bart, Instant.parse("2000-01-01T01:04:00Z"));

        List<KeyValue<String, KafkaUserAggregate>> results = outputTopic.readKeyValuesToList();

        // Homer arrives
        assertEquals("Simpson@2000-01-01T00:55:00Z->2000-01-01T01:00:00Z", results.get(0).key);
        assertIterableEquals(List.of(homer), results.get(0).value.getUsers());

        // Marge arrives.
        assertEquals("Simpson@2000-01-01T01:00:00.001Z->2000-01-01T01:05:00.001Z", results.get(1).key);
        assertIterableEquals(List.of(marge), results.get(1).value.getUsers());

        assertEquals("Simpson@2000-01-01T00:57:00Z->2000-01-01T01:02:00Z", results.get(2).key);
        assertIterableEquals(List.of(homer, marge), results.get(2).value.getUsers());

        // Bart arrives
        assertEquals("Simpson@2000-01-01T01:00:00.001Z->2000-01-01T01:05:00.001Z", results.get(3).key);
        assertIterableEquals(List.of(marge, bart), results.get(3).value.getUsers());

        assertEquals("Simpson@2000-01-01T01:02:00.001Z->2000-01-01T01:07:00.001Z", results.get(4).key);
        assertIterableEquals(List.of(bart), results.get(4).value.getUsers());

        assertEquals("Simpson@2000-01-01T00:59:00Z->2000-01-01T01:04:00Z", results.get(5).key);
        assertIterableEquals(List.of(homer, marge, bart), results.get(5).value.getUsers());

        WindowStore<String, KafkaUserAggregate> stateStore =
                testDriver.getWindowStore(USER_AGGREGATE_SLIDING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaUserAggregate> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaUserAggregate> keyValueSimpson55To00 = iterator.next();
            assertEquals("Simpson", keyValueSimpson55To00.key.key());
            assertEquals(
                    "2000-01-01T00:55:00Z",
                    keyValueSimpson55To00.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    keyValueSimpson55To00.key.window().endTime().toString());
            assertIterableEquals(List.of(homer), keyValueSimpson55To00.value.getUsers());

            KeyValue<Windowed<String>, KafkaUserAggregate> keyValueSimpson57To02 = iterator.next();
            assertEquals("Simpson", keyValueSimpson57To02.key.key());
            assertEquals(
                    "2000-01-01T00:57:00Z",
                    keyValueSimpson57To02.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:02:00Z",
                    keyValueSimpson57To02.key.window().endTime().toString());
            assertIterableEquals(List.of(homer, marge), keyValueSimpson57To02.value.getUsers());

            KeyValue<Windowed<String>, KafkaUserAggregate> keyValueSimpson59To04 = iterator.next();
            assertEquals("Simpson", keyValueSimpson59To04.key.key());
            assertEquals(
                    "2000-01-01T00:59:00Z",
                    keyValueSimpson59To04.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:04:00Z",
                    keyValueSimpson59To04.key.window().endTime().toString());
            assertIterableEquals(List.of(homer, marge, bart), keyValueSimpson59To04.value.getUsers());

            KeyValue<Windowed<String>, KafkaUserAggregate> keyValueSimpson00To05 = iterator.next();
            assertEquals("Simpson", keyValueSimpson00To05.key.key());
            assertEquals(
                    "2000-01-01T01:00:00.001Z",
                    keyValueSimpson00To05.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:05:00.001Z",
                    keyValueSimpson00To05.key.window().endTime().toString());
            assertIterableEquals(List.of(marge, bart), keyValueSimpson00To05.value.getUsers());

            KeyValue<Windowed<String>, KafkaUserAggregate> keyValueSimpson02To07 = iterator.next();
            assertEquals("Simpson", keyValueSimpson02To07.key.key());
            assertEquals(
                    "2000-01-01T01:02:00.001Z",
                    keyValueSimpson02To07.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:07:00.001Z",
                    keyValueSimpson02To07.key.window().endTime().toString());
            assertIterableEquals(List.of(bart), keyValueSimpson02To07.value.getUsers());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldNotAggregateWhenTimeWindowIsNotRespected() {
        KafkaUser homer = buildKafkaUser("Homer");
        inputTopic.pipeInput(new TestRecord<>("1", homer, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaUser marge = buildKafkaUser("Marge");
        inputTopic.pipeInput(new TestRecord<>("2", marge, Instant.parse("2000-01-01T01:05:01Z")));

        List<KeyValue<String, KafkaUserAggregate>> results = outputTopic.readKeyValuesToList();

        assertEquals("Simpson@2000-01-01T00:55:00Z->2000-01-01T01:00:00Z", results.get(0).key);
        assertIterableEquals(List.of(homer), results.get(0).value.getUsers());

        assertEquals("Simpson@2000-01-01T01:00:01Z->2000-01-01T01:05:01Z", results.get(1).key);
        assertIterableEquals(List.of(marge), results.get(1).value.getUsers());

        WindowStore<String, KafkaUserAggregate> stateStore =
                testDriver.getWindowStore(USER_AGGREGATE_SLIDING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaUserAggregate> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaUserAggregate> keyValue00To05 = iterator.next();
            assertEquals("Simpson", keyValue00To05.key.key());
            assertEquals(
                    "2000-01-01T00:55:00Z",
                    keyValue00To05.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of(homer), keyValue00To05.value.getUsers());

            KeyValue<Windowed<String>, KafkaUserAggregate> keyValue01To06 = iterator.next();
            assertEquals("Simpson", keyValue01To06.key.key());
            assertEquals(
                    "2000-01-01T01:00:01Z",
                    keyValue01To06.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:05:01Z",
                    keyValue01To06.key.window().endTime().toString());
            assertIterableEquals(List.of(marge), keyValue01To06.value.getUsers());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        KafkaUser homer = buildKafkaUser("Homer");
        inputTopic.pipeInput(new TestRecord<>("1", homer, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaUser marge = buildKafkaUser("Marge");
        inputTopic.pipeInput(new TestRecord<>("3", marge, Instant.parse("2000-01-01T01:05:30Z")));

        // At this point, the stream time is 01:05:30. It exceeds by 30 seconds
        // the upper bound of the Homer's window [01:00:00.001Z->01:05:00.001Z] where Bart should be included.
        // However, the following delayed record "Bart" will be aggregated into the window
        // because the grace period is 1 minute.

        KafkaUser bart = buildKafkaUser("Bart");
        inputTopic.pipeInput(new TestRecord<>("2", bart, Instant.parse("2000-01-01T01:03:00Z")));

        List<KeyValue<String, KafkaUserAggregate>> results = outputTopic.readKeyValuesToList();

        // Homer arrives
        assertEquals("Simpson@2000-01-01T00:55:00Z->2000-01-01T01:00:00Z", results.get(0).key);
        assertIterableEquals(List.of(homer), results.get(0).value.getUsers());

        // Marge arrives
        assertEquals("Simpson@2000-01-01T01:00:30Z->2000-01-01T01:05:30Z", results.get(1).key);
        assertIterableEquals(List.of(marge), results.get(1).value.getUsers());

        // Bart arrives
        assertEquals("Simpson@2000-01-01T01:00:30Z->2000-01-01T01:05:30Z", results.get(2).key);
        assertIterableEquals(List.of(marge, bart), results.get(2).value.getUsers());

        // Even if the stream time is 01:05:30, the Homer's window [01:00:00.001Z->01:05:00.001Z] is
        // not yet closed thanks to the grace period of 1 minute.
        // Bart whose timestamp is 01:03:00 is included in the window.
        assertEquals("Simpson@2000-01-01T01:00:00.001Z->2000-01-01T01:05:00.001Z", results.get(3).key);
        assertIterableEquals(List.of(bart), results.get(3).value.getUsers());

        assertEquals("Simpson@2000-01-01T01:03:00.001Z->2000-01-01T01:08:00.001Z", results.get(4).key);
        assertIterableEquals(List.of(marge, bart), results.get(4).value.getUsers());

        WindowStore<String, KafkaUserAggregate> stateStore =
                testDriver.getWindowStore(USER_AGGREGATE_SLIDING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaUserAggregate> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaUserAggregate> keyValue55To00 = iterator.next();
            assertEquals("Simpson", keyValue55To00.key.key());
            assertEquals(
                    "2000-01-01T00:55:00Z",
                    keyValue55To00.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    keyValue55To00.key.window().endTime().toString());
            assertIterableEquals(List.of(homer), keyValue55To00.value.getUsers());

            KeyValue<Windowed<String>, KafkaUserAggregate> keyValue00To05 = iterator.next();
            assertEquals("Simpson", keyValue00To05.key.key());
            assertEquals(
                    "2000-01-01T01:00:00.001Z",
                    keyValue00To05.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:05:00.001Z",
                    keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of(bart), keyValue00To05.value.getUsers());

            KeyValue<Windowed<String>, KafkaUserAggregate> keyValue00m30To05m30 = iterator.next();
            assertEquals("Simpson", keyValue00m30To05m30.key.key());
            assertEquals(
                    "2000-01-01T01:00:30Z",
                    keyValue00m30To05m30.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:05:30Z",
                    keyValue00m30To05m30.key.window().endTime().toString());
            assertIterableEquals(List.of(marge, bart), keyValue00m30To05m30.value.getUsers());

            KeyValue<Windowed<String>, KafkaUserAggregate> keyValue03To08 = iterator.next();
            assertEquals("Simpson", keyValue03To08.key.key());
            assertEquals(
                    "2000-01-01T01:03:00.001Z",
                    keyValue03To08.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:08:00.001Z",
                    keyValue03To08.key.window().endTime().toString());
            assertIterableEquals(List.of(marge, bart), keyValue03To08.value.getUsers());

            assertFalse(iterator.hasNext());
        }
    }

    private KafkaUser buildKafkaUser(String firstName) {
        return KafkaUser.newBuilder()
                .setId(1L)
                .setFirstName(firstName)
                .setLastName("Simpson")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
                .build();
    }
}
