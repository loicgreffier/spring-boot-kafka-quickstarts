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
package io.github.loicgreffier.streams.aggregate.tumbling.window;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.aggregate.tumbling.window.constant.StateStore.USER_AGGREGATE_TUMBLING_WINDOW_STORE;
import static io.github.loicgreffier.streams.aggregate.tumbling.window.constant.Topic.USER_AGGREGATE_TUMBLING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.tumbling.window.constant.Topic.USER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.avro.KafkaUserAggregate;
import io.github.loicgreffier.streams.aggregate.tumbling.window.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.aggregate.tumbling.window.serdes.SerdesUtils;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsAggregateTumblingWindowApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsAggregateTumblingWindowApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaUser> inputTopic;
    private TestOutputTopic<String, KafkaUserAggregate> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-aggregate-tumbling-window-test");
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
                USER_AGGREGATE_TUMBLING_WINDOW_TOPIC,
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
        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(0).key);
        assertIterableEquals(List.of(homer), results.get(0).value.getUsers());

        // Marge arrives
        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(1).key);
        assertIterableEquals(List.of(homer, marge), results.get(1).value.getUsers());

        // Bart arrives
        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(2).key);
        assertIterableEquals(List.of(homer, marge, bart), results.get(2).value.getUsers());

        WindowStore<String, KafkaUserAggregate> stateStore =
                testDriver.getWindowStore(USER_AGGREGATE_TUMBLING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaUserAggregate> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaUserAggregate> keyValue00To05 = iterator.next();
            assertEquals("Simpson", keyValue00To05.key.key());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    keyValue00To05.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:05:00Z",
                    keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of(homer, marge, bart), keyValue00To05.value.getUsers());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldNotAggregateWhenTimeWindowIsNotRespected() {
        KafkaUser homer = buildKafkaUser("Homer");
        inputTopic.pipeInput("1", homer, Instant.parse("2000-01-01T01:00:00Z"));

        KafkaUser marge = buildKafkaUser("Marge");
        inputTopic.pipeInput("2", marge, Instant.parse("2000-01-01T01:05:00Z"));

        List<KeyValue<String, KafkaUserAggregate>> results = outputTopic.readKeyValuesToList();

        // The second record is not aggregated here because it is out of the time window
        // as the upper bound of tumbling window is exclusive.
        // Its timestamp (01:05:00) is not included in the window [01:00:00->01:05:00).

        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(0).key);
        assertIterableEquals(List.of(homer), results.get(0).value.getUsers());

        assertEquals("Simpson@2000-01-01T01:05:00Z->2000-01-01T01:10:00Z", results.get(1).key);
        assertIterableEquals(List.of(marge), results.get(1).value.getUsers());

        WindowStore<String, KafkaUserAggregate> stateStore =
                testDriver.getWindowStore(USER_AGGREGATE_TUMBLING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaUserAggregate> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaUserAggregate> keyValue00To05 = iterator.next();
            assertEquals("Simpson", keyValue00To05.key.key());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    keyValue00To05.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:05:00Z",
                    keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of(homer), keyValue00To05.value.getUsers());

            KeyValue<Windowed<String>, KafkaUserAggregate> keyValue05To10 = iterator.next();
            assertEquals("Simpson", keyValue05To10.key.key());
            assertEquals(
                    "2000-01-01T01:05:00Z",
                    keyValue05To10.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:10:00Z",
                    keyValue05To10.key.window().endTime().toString());
            assertIterableEquals(List.of(marge), keyValue05To10.value.getUsers());

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        KafkaUser homer = buildKafkaUser("Homer");
        inputTopic.pipeInput("1", homer, Instant.parse("2000-01-01T01:00:00Z"));

        KafkaUser marge = buildKafkaUser("Marge");
        inputTopic.pipeInput("3", marge, Instant.parse("2000-01-01T01:05:30Z"));

        // At this point, the stream time is 01:05:30. It exceeds by 30 seconds
        // the upper bound of the window [01:00:00Z->01:05:00Z) where Homer is included.
        // However, the following delayed record "Bart" will be aggregated into the window
        // because the grace period is 1 minute.

        KafkaUser bart = buildKafkaUser("Bart");
        inputTopic.pipeInput("2", bart, Instant.parse("2000-01-01T01:03:00Z"));

        List<KeyValue<String, KafkaUserAggregate>> results = outputTopic.readKeyValuesToList();

        // Homer arrives
        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(0).key);
        assertIterableEquals(List.of(homer), results.get(0).value.getUsers());

        // Marge arrives
        assertEquals("Simpson@2000-01-01T01:05:00Z->2000-01-01T01:10:00Z", results.get(1).key);
        assertIterableEquals(List.of(marge), results.get(1).value.getUsers());

        // Bart arrives
        // Even if the stream time is 01:05:30, the window [01:00:00Z->01:05:00Z) is
        // not yet closed because of the grace period of 1 minute.
        // Bart whose timestamp is 01:03:00 is included in the window.
        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(2).key);
        assertIterableEquals(List.of(homer, bart), results.get(2).value.getUsers());

        WindowStore<String, KafkaUserAggregate> stateStore =
                testDriver.getWindowStore(USER_AGGREGATE_TUMBLING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaUserAggregate> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaUserAggregate> keyValue00To05 = iterator.next();
            assertEquals("Simpson", keyValue00To05.key.key());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    keyValue00To05.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:05:00Z",
                    keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of(homer, bart), keyValue00To05.value.getUsers());

            KeyValue<Windowed<String>, KafkaUserAggregate> keyValue05To10 = iterator.next();
            assertEquals("Simpson", keyValue05To10.key.key());
            assertEquals(
                    "2000-01-01T01:05:00Z",
                    keyValue05To10.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:10:00Z",
                    keyValue05To10.key.window().endTime().toString());
            assertIterableEquals(List.of(marge), keyValue05To10.value.getUsers());

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
