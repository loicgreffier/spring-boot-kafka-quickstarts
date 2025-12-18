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
import static io.github.loicgreffier.streams.join.stream.stream.constant.StateStore.USER_JOIN_STREAM_STREAM_STORE;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.USER_JOIN_STREAM_STREAM_REKEY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.USER_JOIN_STREAM_STREAM_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.USER_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.USER_TOPIC_TWO;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.KafkaJoinUsers;
import io.github.loicgreffier.avro.KafkaUser;
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
    private TestInputTopic<String, KafkaUser> leftInputTopic;
    private TestInputTopic<String, KafkaUser> rightInputTopic;
    private TestOutputTopic<String, KafkaUser> rekeyLeftOutputTopic;
    private TestOutputTopic<String, KafkaUser> rekeyRightOutputTopic;
    private TestOutputTopic<String, KafkaJoinUsers> joinOutputTopic;

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

        leftInputTopic = testDriver.createInputTopic(
                USER_TOPIC,
                new StringSerializer(),
                SerdesUtils.<KafkaUser>getValueSerdes().serializer());
        rightInputTopic = testDriver.createInputTopic(
                USER_TOPIC_TWO,
                new StringSerializer(),
                SerdesUtils.<KafkaUser>getValueSerdes().serializer());
        rekeyLeftOutputTopic = testDriver.createOutputTopic(
                "streams-join-stream-stream-test-" + USER_JOIN_STREAM_STREAM_REKEY_TOPIC + "-left-repartition",
                new StringDeserializer(),
                SerdesUtils.<KafkaUser>getValueSerdes().deserializer());
        rekeyRightOutputTopic = testDriver.createOutputTopic(
                "streams-join-stream-stream-test-" + USER_JOIN_STREAM_STREAM_REKEY_TOPIC + "-right-repartition",
                new StringDeserializer(),
                SerdesUtils.<KafkaUser>getValueSerdes().deserializer());
        joinOutputTopic = testDriver.createOutputTopic(
                USER_JOIN_STREAM_STREAM_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<KafkaJoinUsers>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Path.of(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldRekey() {
        KafkaUser leftUser = buildKafkaUser("Homer");
        KafkaUser rightUser = buildKafkaUser("Marge");

        leftInputTopic.pipeInput("1", leftUser);
        rightInputTopic.pipeInput("2", rightUser);

        List<KeyValue<String, KafkaUser>> topicOneResults = rekeyLeftOutputTopic.readKeyValuesToList();
        List<KeyValue<String, KafkaUser>> topicTwoResults = rekeyRightOutputTopic.readKeyValuesToList();

        assertEquals(KeyValue.pair("Simpson", leftUser), topicOneResults.getFirst());
        assertEquals(KeyValue.pair("Simpson", rightUser), topicTwoResults.getFirst());
    }

    @Test
    void shouldJoinWhenTimeWindowIsRespected() {
        KafkaUser homer = buildKafkaUser("Homer");
        leftInputTopic.pipeInput(new TestRecord<>("1", homer, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaUser marge = buildKafkaUser("Marge");
        rightInputTopic.pipeInput(new TestRecord<>("2", marge, Instant.parse("2000-01-01T01:02:00Z")));

        KafkaUser bart = buildKafkaUser("Bart");
        leftInputTopic.pipeInput(new TestRecord<>("3", bart, Instant.parse("2000-01-01T01:03:00Z")));

        List<KeyValue<String, KafkaJoinUsers>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("Simpson", results.getFirst().key);
        assertEquals(homer, results.get(0).value.getUserOne());
        assertEquals(marge, results.get(0).value.getUserTwo());

        assertEquals("Simpson", results.get(1).key);
        assertEquals(bart, results.get(1).value.getUserOne());
        assertEquals(marge, results.get(1).value.getUserTwo());

        WindowStore<String, KafkaUser> leftStateStore =
                testDriver.getWindowStore(USER_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaUser> iterator = leftStateStore.all()) {
            // As join windows are looking backward and forward in time,
            // records are kept in the store for "before" + "after" duration.

            KeyValue<Windowed<String>, KafkaUser> leftKeyValue00To10 = iterator.next();
            assertEquals("Simpson", leftKeyValue00To10.key.key());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    leftKeyValue00To10.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:10:00Z",
                    leftKeyValue00To10.key.window().endTime().toString());
            assertEquals(homer, leftKeyValue00To10.value);

            KeyValue<Windowed<String>, KafkaUser> leftKeyValue03To13 = iterator.next();
            assertEquals("Simpson", leftKeyValue03To13.key.key());
            assertEquals(
                    "2000-01-01T01:03:00Z",
                    leftKeyValue03To13.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:13:00Z",
                    leftKeyValue03To13.key.window().endTime().toString());
            assertEquals(bart, leftKeyValue03To13.value);

            assertFalse(iterator.hasNext());
        }

        WindowStore<String, KafkaUser> rightStateStore =
                testDriver.getWindowStore(USER_JOIN_STREAM_STREAM_STORE + "-other-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaUser> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaUser> rightKeyValue02To12 = iterator.next();
            assertEquals("Simpson", rightKeyValue02To12.key.key());
            assertEquals(
                    "2000-01-01T01:02:00Z",
                    rightKeyValue02To12.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:12:00Z",
                    rightKeyValue02To12.key.window().endTime().toString());
            assertEquals(marge, rightKeyValue02To12.value);

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldNotJoinWhenTimeWindowIsNotRespected() {
        KafkaUser homer = buildKafkaUser("Homer");
        leftInputTopic.pipeInput(new TestRecord<>("1", homer, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaUser marge = buildKafkaUser("Marge");
        rightInputTopic.pipeInput(new TestRecord<>("2", marge, Instant.parse("2000-01-01T01:05:01Z")));

        KafkaUser bart = buildKafkaUser("Bart");
        leftInputTopic.pipeInput(new TestRecord<>("3", bart, Instant.parse("2000-01-01T01:10:02Z")));

        List<KeyValue<String, KafkaJoinUsers>> results = joinOutputTopic.readKeyValuesToList();

        // No records joined because Marge arrived too late for Homer and Bart arrived too late for Marge.
        assertTrue(results.isEmpty());

        WindowStore<String, KafkaUser> leftStateStore =
                testDriver.getWindowStore(USER_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaUser> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaUser> leftKeyValue00To10 = iterator.next();
            assertEquals("Simpson", leftKeyValue00To10.key.key());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    leftKeyValue00To10.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:10:00Z",
                    leftKeyValue00To10.key.window().endTime().toString());
            assertEquals(homer, leftKeyValue00To10.value);

            KeyValue<Windowed<String>, KafkaUser> leftKeyValue10To20 = iterator.next();
            assertEquals("Simpson", leftKeyValue10To20.key.key());
            assertEquals(
                    "2000-01-01T01:10:02Z",
                    leftKeyValue10To20.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:20:02Z",
                    leftKeyValue10To20.key.window().endTime().toString());
            assertEquals(bart, leftKeyValue10To20.value);

            assertFalse(iterator.hasNext());
        }

        WindowStore<String, KafkaUser> rightStateStore =
                testDriver.getWindowStore(USER_JOIN_STREAM_STREAM_STORE + "-other-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaUser> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaUser> rightKeyValue = iterator.next();
            assertEquals("Simpson", rightKeyValue.key.key());
            assertEquals(
                    "2000-01-01T01:05:01Z",
                    rightKeyValue.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:15:01Z", rightKeyValue.key.window().endTime().toString());
            assertEquals(marge, rightKeyValue.value);

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        KafkaUser homer = buildKafkaUser("Homer");
        leftInputTopic.pipeInput(new TestRecord<>("1", homer, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaUser marge = buildKafkaUser("Marge");
        leftInputTopic.pipeInput(new TestRecord<>("3", marge, Instant.parse("2000-01-01T01:10:30Z")));

        // At this point, the stream time is 01:10:30. It exceeds by 30 seconds
        // the upper bound of the Homer's window [01:00:00.001Z->01:10:00Z] in the store.
        // However, the following delayed record "Bart" will be joined with the first record
        // thanks to the grace period of 1 minute.

        KafkaUser bart = buildKafkaUser("Bart");
        rightInputTopic.pipeInput(new TestRecord<>("2", bart, Instant.parse("2000-01-01T01:05:00Z")));

        List<KeyValue<String, KafkaJoinUsers>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("Simpson", results.getFirst().key);
        assertEquals(homer, results.getFirst().value.getUserOne());
        assertEquals(bart, results.getFirst().value.getUserTwo());

        WindowStore<String, KafkaUser> leftStateStore =
                testDriver.getWindowStore(USER_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaUser> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaUser> leftKeyValue00To10 = iterator.next();
            assertEquals("Simpson", leftKeyValue00To10.key.key());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    leftKeyValue00To10.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:10:00Z",
                    leftKeyValue00To10.key.window().endTime().toString());
            assertEquals(homer, leftKeyValue00To10.value);

            KeyValue<Windowed<String>, KafkaUser> leftKeyValue10m30To20m30 = iterator.next();
            assertEquals("Simpson", leftKeyValue10m30To20m30.key.key());
            assertEquals(
                    "2000-01-01T01:10:30Z",
                    leftKeyValue10m30To20m30.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:20:30Z",
                    leftKeyValue10m30To20m30.key.window().endTime().toString());
            assertEquals(marge, leftKeyValue10m30To20m30.value);

            assertFalse(iterator.hasNext());
        }

        WindowStore<String, KafkaUser> rightStateStore =
                testDriver.getWindowStore(USER_JOIN_STREAM_STREAM_STORE + "-other-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaUser> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaUser> rightKeyValue = iterator.next();
            assertEquals("Simpson", rightKeyValue.key.key());
            assertEquals(
                    "2000-01-01T01:05:00Z",
                    rightKeyValue.key.window().startTime().toString());
            assertEquals(
                    "2000-01-01T01:15:00Z", rightKeyValue.key.window().endTime().toString());
            assertEquals(bart, rightKeyValue.value);

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
