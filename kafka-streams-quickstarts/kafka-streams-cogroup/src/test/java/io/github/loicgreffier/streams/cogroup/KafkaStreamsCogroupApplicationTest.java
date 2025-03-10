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
package io.github.loicgreffier.streams.cogroup;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.cogroup.constant.StateStore.USER_COGROUP_AGGREGATE_STORE;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.USER_COGROUP_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.USER_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.USER_TOPIC_TWO;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.avro.KafkaUserGroup;
import io.github.loicgreffier.streams.cogroup.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.cogroup.serdes.SerdesUtils;
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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsCogroupApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsCogroupApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaUser> inputTopicOne;
    private TestInputTopic<String, KafkaUser> inputTopicTwo;
    private TestOutputTopic<String, KafkaUserGroup> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-cogroup-test");
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

        inputTopicOne = testDriver.createInputTopic(
                USER_TOPIC,
                new StringSerializer(),
                SerdesUtils.<KafkaUser>getValueSerdes().serializer());
        inputTopicTwo = testDriver.createInputTopic(
                USER_TOPIC_TWO,
                new StringSerializer(),
                SerdesUtils.<KafkaUser>getValueSerdes().serializer());
        outputTopic = testDriver.createOutputTopic(
                USER_COGROUP_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<KafkaUserGroup>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldAggregateFirstNamesByLastNameStreamOne() {
        inputTopicOne.pipeInput(new TestRecord<>("1", buildKafkaUser("Homer"), Instant.parse("2000-01-01T01:00:00Z")));
        inputTopicOne.pipeInput(new TestRecord<>("2", buildKafkaUser("Marge"), Instant.parse("2000-01-01T01:00:00Z")));

        List<KeyValue<String, KafkaUserGroup>> results = outputTopic.readKeyValuesToList();

        assertEquals("Simpson", results.get(0).key);
        assertIterableEquals(
                List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson", results.get(1).key);
        assertIterableEquals(
                List.of("Homer", "Marge"),
                results.get(1).value.getFirstNameByLastName().get("Simpson"));

        KeyValueStore<String, KafkaUserGroup> stateStore = testDriver.getKeyValueStore(USER_COGROUP_AGGREGATE_STORE);

        assertIterableEquals(
                List.of("Homer", "Marge"),
                stateStore.get("Simpson").getFirstNameByLastName().get("Simpson"));
    }

    @Test
    void shouldAggregateFirstNamesByLastNameStreamTwo() {
        inputTopicTwo.pipeInput(new TestRecord<>("1", buildKafkaUser("Homer"), Instant.parse("2000-01-01T01:00:00Z")));
        inputTopicTwo.pipeInput(new TestRecord<>("2", buildKafkaUser("Marge"), Instant.parse("2000-01-01T01:00:00Z")));

        List<KeyValue<String, KafkaUserGroup>> results = outputTopic.readKeyValuesToList();

        assertEquals("Simpson", results.get(0).key);
        assertIterableEquals(
                List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson", results.get(1).key);
        assertIterableEquals(
                List.of("Homer", "Marge"),
                results.get(1).value.getFirstNameByLastName().get("Simpson"));

        KeyValueStore<String, KafkaUserGroup> stateStore = testDriver.getKeyValueStore(USER_COGROUP_AGGREGATE_STORE);

        assertIterableEquals(
                List.of("Homer", "Marge"),
                stateStore.get("Simpson").getFirstNameByLastName().get("Simpson"));
    }

    @Test
    void shouldAggregateFirstNamesByLastNameBothCogroupedStreams() {
        inputTopicOne.pipeInput(new TestRecord<>("1", buildKafkaUser("Homer"), Instant.parse("2000-01-01T01:00:00Z")));
        inputTopicOne.pipeInput(new TestRecord<>("2", buildKafkaUser("Marge"), Instant.parse("2000-01-01T01:00:00Z")));
        inputTopicTwo.pipeInput(new TestRecord<>("3", buildKafkaUser("Bart"), Instant.parse("2000-01-01T01:00:00Z")));

        List<KeyValue<String, KafkaUserGroup>> results = outputTopic.readKeyValuesToList();

        assertEquals("Simpson", results.get(0).key);
        assertIterableEquals(
                List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson", results.get(1).key);
        assertIterableEquals(
                List.of("Homer", "Marge"),
                results.get(1).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson", results.get(2).key);
        assertIterableEquals(
                List.of("Homer", "Marge", "Bart"),
                results.get(2).value.getFirstNameByLastName().get("Simpson"));

        KeyValueStore<String, KafkaUserGroup> stateStore = testDriver.getKeyValueStore(USER_COGROUP_AGGREGATE_STORE);

        assertIterableEquals(
                List.of("Homer", "Marge", "Bart"),
                stateStore.get("Simpson").getFirstNameByLastName().get("Simpson"));
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
