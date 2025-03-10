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
package io.github.loicgreffier.streams.schedule;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.schedule.constant.StateStore.USER_SCHEDULE_STORE;
import static io.github.loicgreffier.streams.schedule.constant.Topic.USER_SCHEDULE_TOPIC;
import static io.github.loicgreffier.streams.schedule.constant.Topic.USER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.schedule.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.schedule.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsScheduleApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsScheduleApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaUser> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-schedule-test");
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
        outputTopic =
                testDriver.createOutputTopic(USER_SCHEDULE_TOPIC, new StringDeserializer(), new LongDeserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldCountUserByNationality() {
        inputTopic.pipeInput(new TestRecord<>(
                "1", buildKafkaUser("Homer", "Simpson", CountryCode.US), Instant.parse("2000-01-01T01:00:00Z")));

        inputTopic.pipeInput(new TestRecord<>(
                "2", buildKafkaUser("Marge", "Simpson", CountryCode.US), Instant.parse("2000-01-01T01:01:00Z")));

        inputTopic.pipeInput(new TestRecord<>(
                "3", buildKafkaUser("Milhouse", "Van Houten", CountryCode.BE), Instant.parse("2000-01-01T01:01:30Z")));

        inputTopic.pipeInput(new TestRecord<>(
                "4", buildKafkaUser("Luigi", "Risotto", CountryCode.IT), Instant.parse("2000-01-01T01:02:00Z")));

        testDriver.advanceWallClockTime(Duration.ofMinutes(2));

        inputTopic.pipeInput(new TestRecord<>(
                "5", buildKafkaUser("Bart", "Simpson", CountryCode.US), Instant.parse("2000-01-01T01:04:00Z")));

        List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

        // 1st stream time punctuate
        assertEquals("US", results.get(0).key);
        assertEquals(1, results.get(0).value);

        // 2nd stream time punctuate
        assertEquals("US", results.get(1).key);
        assertEquals(2, results.get(1).value);

        // 3rd stream time punctuate
        assertEquals("BE", results.get(2).key);
        assertEquals(1, results.get(2).value);

        assertEquals("IT", results.get(3).key);
        assertEquals(1, results.get(3).value);

        assertEquals("US", results.get(4).key);
        assertEquals(2, results.get(4).value);

        // 1st wall clock time punctuate now

        // 4th stream time punctuate
        assertEquals("BE", results.get(5).key);
        assertEquals(0, results.get(5).value);

        assertEquals("IT", results.get(6).key);
        assertEquals(0, results.get(6).value);

        assertEquals("US", results.get(7).key);
        assertEquals(1, results.get(7).value);

        KeyValueStore<String, Long> stateStore = testDriver.getKeyValueStore(USER_SCHEDULE_STORE);

        assertEquals(1, stateStore.get("US"));
        assertEquals(0, stateStore.get("BE"));
        assertEquals(0, stateStore.get("IT"));
    }

    private KafkaUser buildKafkaUser(String firstName, String lastName, CountryCode nationality) {
        return KafkaUser.newBuilder()
                .setId(1L)
                .setFirstName(firstName)
                .setLastName(lastName)
                .setNationality(nationality)
                .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
                .build();
    }
}
