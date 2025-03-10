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
package io.github.loicgreffier.streams.store.window.timestamped;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.store.window.timestamped.constant.StateStore.USER_TIMESTAMPED_WINDOW_STORE;
import static io.github.loicgreffier.streams.store.window.timestamped.constant.StateStore.USER_TIMESTAMPED_WINDOW_SUPPLIER_STORE;
import static io.github.loicgreffier.streams.store.window.timestamped.constant.Topic.USER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.store.window.timestamped.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.store.window.timestamped.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaStreamsStoreWindowTimestampedApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsStoreWindowTimestampedApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaUser> inputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-schedule-store-window-timestamped-test");
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
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @ParameterizedTest
    @ValueSource(strings = {USER_TIMESTAMPED_WINDOW_STORE, USER_TIMESTAMPED_WINDOW_SUPPLIER_STORE})
    void shouldPutAndGetFromWindowStores(String storeName) {
        KafkaUser homer = buildKafkaUser("Homer");
        Instant homerTimestamp = Instant.parse("2000-01-01T01:00:00Z");
        inputTopic.pipeInput(new TestRecord<>("1", homer, homerTimestamp));
        inputTopic.pipeInput(new TestRecord<>("1", homer, homerTimestamp.plusSeconds(10)));

        KafkaUser marge = buildKafkaUser("Marge");
        Instant margeTimestamp = Instant.parse("2000-01-01T01:00:30Z");
        inputTopic.pipeInput(new TestRecord<>("2", marge, margeTimestamp));
        inputTopic.pipeInput(new TestRecord<>("2", marge, margeTimestamp.plusSeconds(10)));

        WindowStore<String, ValueAndTimestamp<KafkaUser>> windowStore = testDriver.getTimestampedWindowStore(storeName);

        // Fetch from window store by key and timestamp. The timestamp used to fetch has to be equal to
        // the window start time to get the value.

        assertEquals(
                homer, windowStore.fetch("1", homerTimestamp.toEpochMilli()).value());
        assertEquals(
                "2000-01-01T01:00:00Z",
                Instant.ofEpochMilli(windowStore
                                .fetch("1", homerTimestamp.toEpochMilli())
                                .timestamp())
                        .toString());
        assertEquals(
                homer,
                windowStore
                        .fetch("1", homerTimestamp.plusSeconds(10).toEpochMilli())
                        .value());
        assertEquals(
                "2000-01-01T01:00:10Z",
                Instant.ofEpochMilli(windowStore
                                .fetch("1", homerTimestamp.plusSeconds(10).toEpochMilli())
                                .timestamp())
                        .toString());
        assertNull(windowStore.fetch("1", homerTimestamp.plusSeconds(1).toEpochMilli()));

        // Fetch from window store by key and time range.

        try (WindowStoreIterator<ValueAndTimestamp<KafkaUser>> iterator = windowStore.fetch(
                "1",
                homerTimestamp.minusSeconds(30).toEpochMilli(),
                homerTimestamp.plusSeconds(30).toEpochMilli())) {
            ValueAndTimestamp<KafkaUser> valueAndTimestamp = iterator.next().value;
            assertEquals(homer, valueAndTimestamp.value());
            assertEquals(
                    "2000-01-01T01:00:00Z",
                    Instant.ofEpochMilli(valueAndTimestamp.timestamp()).toString());

            valueAndTimestamp = iterator.next().value;
            assertEquals(homer, valueAndTimestamp.value());
            assertEquals(
                    "2000-01-01T01:00:10Z",
                    Instant.ofEpochMilli(valueAndTimestamp.timestamp()).toString());

            assertFalse(iterator.hasNext());
        }

        assertEquals(
                marge, windowStore.fetch("2", margeTimestamp.toEpochMilli()).value());
        assertEquals(
                "2000-01-01T01:00:30Z",
                Instant.ofEpochMilli(windowStore
                                .fetch("2", margeTimestamp.toEpochMilli())
                                .timestamp())
                        .toString());
        assertEquals(
                marge,
                windowStore
                        .fetch("2", margeTimestamp.plusSeconds(10).toEpochMilli())
                        .value());
        assertEquals(
                "2000-01-01T01:00:40Z",
                Instant.ofEpochMilli(windowStore
                                .fetch("2", margeTimestamp.plusSeconds(10).toEpochMilli())
                                .timestamp())
                        .toString());
        assertNull(windowStore.fetch("2", margeTimestamp.plusSeconds(1).toEpochMilli()));

        try (WindowStoreIterator<ValueAndTimestamp<KafkaUser>> iterator = windowStore.fetch(
                "2",
                margeTimestamp.minusSeconds(30).toEpochMilli(),
                margeTimestamp.plusSeconds(30).toEpochMilli())) {
            ValueAndTimestamp<KafkaUser> valueAndTimestamp = iterator.next().value;
            assertEquals(marge, valueAndTimestamp.value());
            assertEquals(
                    "2000-01-01T01:00:30Z",
                    Instant.ofEpochMilli(valueAndTimestamp.timestamp()).toString());

            valueAndTimestamp = iterator.next().value;
            assertEquals(marge, valueAndTimestamp.value());
            assertEquals(
                    "2000-01-01T01:00:40Z",
                    Instant.ofEpochMilli(valueAndTimestamp.timestamp()).toString());

            assertFalse(iterator.hasNext());
        }
    }

    private KafkaUser buildKafkaUser(String firstName) {
        return KafkaUser.newBuilder()
                .setId(1L)
                .setFirstName(firstName)
                .setLastName("Simpson")
                .setNationality(CountryCode.GB)
                .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
                .build();
    }
}
