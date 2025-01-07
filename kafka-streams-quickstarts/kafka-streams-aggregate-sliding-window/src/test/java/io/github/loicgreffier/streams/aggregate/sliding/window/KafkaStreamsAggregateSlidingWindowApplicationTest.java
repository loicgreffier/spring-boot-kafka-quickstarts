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
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.StateStore.PERSON_AGGREGATE_SLIDING_WINDOW_STORE;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.PERSON_AGGREGATE_SLIDING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
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
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPersonGroup> outputTopic;

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
        testDriver = new TopologyTestDriver(
            streamsBuilder.build(),
            properties,
            Instant.parse("2000-01-01T01:00:00Z")
        );

        inputTopic = testDriver.createInputTopic(
            PERSON_TOPIC,
            new StringSerializer(),
            SerdesUtils.<KafkaPerson>getValueSerdes().serializer()
        );
        outputTopic = testDriver.createOutputTopic(
            PERSON_AGGREGATE_SLIDING_WINDOW_TOPIC,
            new StringDeserializer(),
            SerdesUtils.<KafkaPersonGroup>getValueSerdes().deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldAggregateWhenTimeWindowIsRespected() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("Homer"), Instant.parse("2000-01-01T01:00:00Z")));
        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Marge"), Instant.parse("2000-01-01T01:02:00Z")));
        inputTopic.pipeInput(new TestRecord<>("3", buildKafkaPerson("Bart"), Instant.parse("2000-01-01T01:04:00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        // Homer arrives
        assertEquals("Simpson@2000-01-01T00:55:00Z->2000-01-01T01:00:00Z", results.get(0).key);
        assertIterableEquals(List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        // Marge arrives.
        assertEquals("Simpson@2000-01-01T01:00:00.001Z->2000-01-01T01:05:00.001Z", results.get(1).key);
        assertIterableEquals(List.of("Marge"), results.get(1).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T00:57:00Z->2000-01-01T01:02:00Z", results.get(2).key);
        assertIterableEquals(List.of("Homer", "Marge"), results.get(2).value.getFirstNameByLastName().get("Simpson"));

        // Bart arrives
        assertEquals("Simpson@2000-01-01T01:00:00.001Z->2000-01-01T01:05:00.001Z", results.get(3).key);
        assertIterableEquals(List.of("Marge", "Bart"), results.get(3).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:02:00.001Z->2000-01-01T01:07:00.001Z", results.get(4).key);
        assertIterableEquals(List.of("Bart"), results.get(4).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T00:59:00Z->2000-01-01T01:04:00Z", results.get(5).key);
        assertIterableEquals(
            List.of("Homer", "Marge", "Bart"),
            results.get(5).value.getFirstNameByLastName().get("Simpson")
        );

        WindowStore<String, KafkaPersonGroup> stateStore =
            testDriver.getWindowStore(PERSON_AGGREGATE_SLIDING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueSimpson55To00 = iterator.next();
            assertEquals("Simpson", keyValueSimpson55To00.key.key());
            assertEquals("2000-01-01T00:55:00Z", keyValueSimpson55To00.key.window().startTime().toString());
            assertEquals("2000-01-01T01:00:00Z", keyValueSimpson55To00.key.window().endTime().toString());
            assertIterableEquals(List.of("Homer"), keyValueSimpson55To00.value.getFirstNameByLastName().get("Simpson"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueSimpson57To02 = iterator.next();
            assertEquals("Simpson", keyValueSimpson57To02.key.key());
            assertEquals("2000-01-01T00:57:00Z", keyValueSimpson57To02.key.window().startTime().toString());
            assertEquals("2000-01-01T01:02:00Z", keyValueSimpson57To02.key.window().endTime().toString());
            assertIterableEquals(
                List.of("Homer", "Marge"),
                keyValueSimpson57To02.value.getFirstNameByLastName().get("Simpson")
            );

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueSimpson59To04 = iterator.next();
            assertEquals("Simpson", keyValueSimpson59To04.key.key());
            assertEquals("2000-01-01T00:59:00Z", keyValueSimpson59To04.key.window().startTime().toString());
            assertEquals("2000-01-01T01:04:00Z", keyValueSimpson59To04.key.window().endTime().toString());
            assertIterableEquals(
                List.of("Homer", "Marge", "Bart"),
                keyValueSimpson59To04.value.getFirstNameByLastName().get("Simpson")
            );

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueSimpson00To05 = iterator.next();
            assertEquals("Simpson", keyValueSimpson00To05.key.key());
            assertEquals("2000-01-01T01:00:00.001Z", keyValueSimpson00To05.key.window().startTime().toString());
            assertEquals("2000-01-01T01:05:00.001Z", keyValueSimpson00To05.key.window().endTime().toString());
            assertIterableEquals(
                List.of("Marge", "Bart"),
                keyValueSimpson00To05.value.getFirstNameByLastName().get("Simpson")
            );

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueSimpson02To07 = iterator.next();
            assertEquals("Simpson", keyValueSimpson02To07.key.key());
            assertEquals("2000-01-01T01:02:00.001Z", keyValueSimpson02To07.key.window().startTime().toString());
            assertEquals("2000-01-01T01:07:00.001Z", keyValueSimpson02To07.key.window().endTime().toString());
            assertIterableEquals(List.of("Bart"), keyValueSimpson02To07.value.getFirstNameByLastName().get("Simpson"));

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldNotAggregateWhenTimeWindowIsNotRespected() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("Homer"), Instant.parse("2000-01-01T01:00:00Z")));
        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Marge"), Instant.parse("2000-01-01T01:05:01Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertEquals("Simpson@2000-01-01T00:55:00Z->2000-01-01T01:00:00Z", results.get(0).key);
        assertIterableEquals(List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:00:01Z->2000-01-01T01:05:01Z", results.get(1).key);
        assertIterableEquals(List.of("Marge"), results.get(1).value.getFirstNameByLastName().get("Simpson"));

        WindowStore<String, KafkaPersonGroup> stateStore =
            testDriver.getWindowStore(PERSON_AGGREGATE_SLIDING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue00To05 = iterator.next();
            assertEquals("Simpson", keyValue00To05.key.key());
            assertEquals("2000-01-01T00:55:00Z", keyValue00To05.key.window().startTime().toString());
            assertEquals("2000-01-01T01:00:00Z", keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of("Homer"), keyValue00To05.value.getFirstNameByLastName().get("Simpson"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue01To06 = iterator.next();
            assertEquals("Simpson", keyValue01To06.key.key());
            assertEquals("2000-01-01T01:00:01Z", keyValue01To06.key.window().startTime().toString());
            assertEquals("2000-01-01T01:05:01Z", keyValue01To06.key.window().endTime().toString());
            assertIterableEquals(List.of("Marge"), keyValue01To06.value.getFirstNameByLastName().get("Simpson"));

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("Homer"),
            Instant.parse("2000-01-01T01:00:00Z")));
        inputTopic.pipeInput(new TestRecord<>("3", buildKafkaPerson("Marge"),
            Instant.parse("2000-01-01T01:05:30Z")));

        // At this point, the stream time is 01:05:30. It exceeds by 30 seconds
        // the upper bound of the Homer's window [01:00:00.001Z->01:05:00.001Z] where Bart should be included.
        // However, the following delayed record "Bart" will be aggregated into the window
        // because the grace period is 1 minute.

        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Bart"),
            Instant.parse("2000-01-01T01:03:00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        // Homer arrives
        assertEquals("Simpson@2000-01-01T00:55:00Z->2000-01-01T01:00:00Z", results.get(0).key);
        assertIterableEquals(List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        // Marge arrives
        assertEquals("Simpson@2000-01-01T01:00:30Z->2000-01-01T01:05:30Z", results.get(1).key);
        assertIterableEquals(List.of("Marge"), results.get(1).value.getFirstNameByLastName().get("Simpson"));

        // Bart arrives
        assertEquals("Simpson@2000-01-01T01:00:30Z->2000-01-01T01:05:30Z", results.get(2).key);
        assertIterableEquals(List.of("Marge", "Bart"), results.get(2).value.getFirstNameByLastName().get("Simpson"));

        // Even if the stream time is 01:05:30, the Homer's window [01:00:00.001Z->01:05:00.001Z] is
        // not yet closed thanks to the grace period of 1 minute.
        // Bart whose timestamp is 01:03:00 is included in the window.
        assertEquals("Simpson@2000-01-01T01:00:00.001Z->2000-01-01T01:05:00.001Z", results.get(3).key);
        assertIterableEquals(List.of("Bart"), results.get(3).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:03:00.001Z->2000-01-01T01:08:00.001Z", results.get(4).key);
        assertIterableEquals(List.of("Marge", "Bart"), results.get(4).value.getFirstNameByLastName().get("Simpson"));

        WindowStore<String, KafkaPersonGroup> stateStore =
            testDriver.getWindowStore(PERSON_AGGREGATE_SLIDING_WINDOW_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue55To00 = iterator.next();
            assertEquals("Simpson", keyValue55To00.key.key());
            assertEquals("2000-01-01T00:55:00Z", keyValue55To00.key.window().startTime().toString());
            assertEquals("2000-01-01T01:00:00Z", keyValue55To00.key.window().endTime().toString());
            assertIterableEquals(List.of("Homer"), keyValue55To00.value.getFirstNameByLastName().get("Simpson"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue00To05 = iterator.next();
            assertEquals("Simpson", keyValue00To05.key.key());
            assertEquals("2000-01-01T01:00:00.001Z", keyValue00To05.key.window().startTime().toString());
            assertEquals("2000-01-01T01:05:00.001Z", keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of("Bart"), keyValue00To05.value.getFirstNameByLastName().get("Simpson"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue00m30To05m30 = iterator.next();
            assertEquals("Simpson", keyValue00m30To05m30.key.key());
            assertEquals("2000-01-01T01:00:30Z", keyValue00m30To05m30.key.window().startTime().toString());
            assertEquals("2000-01-01T01:05:30Z", keyValue00m30To05m30.key.window().endTime().toString());
            assertIterableEquals(
                List.of("Marge", "Bart"),
                keyValue00m30To05m30.value.getFirstNameByLastName().get("Simpson")
            );

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue03To08 = iterator.next();
            assertEquals("Simpson", keyValue03To08.key.key());
            assertEquals("2000-01-01T01:03:00.001Z", keyValue03To08.key.window().startTime().toString());
            assertEquals("2000-01-01T01:08:00.001Z", keyValue03To08.key.window().endTime().toString());
            assertIterableEquals(
                List.of("Marge", "Bart"),
                keyValue03To08.value.getFirstNameByLastName().get("Simpson")
            );

            assertFalse(iterator.hasNext());
        }
    }

    private KafkaPerson buildKafkaPerson(String firstName) {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName(firstName)
            .setLastName("Simpson")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build();
    }
}
