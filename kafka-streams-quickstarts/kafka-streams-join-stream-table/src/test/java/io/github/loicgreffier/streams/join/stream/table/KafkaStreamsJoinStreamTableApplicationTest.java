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
package io.github.loicgreffier.streams.join.stream.table;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.COUNTRY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.USER_COUNTRY_JOIN_STREAM_TABLE_TOPIC;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.USER_JOIN_STREAM_TABLE_REKEY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.USER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinUserCountry;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.join.stream.table.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.join.stream.table.serdes.SerdesUtils;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsJoinStreamTableApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsJoinStreamTableApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaUser> userInputTopic;
    private TestInputTopic<String, KafkaCountry> countryInputTopic;
    private TestOutputTopic<String, KafkaUser> rekeyUserOutputTopic;
    private TestOutputTopic<String, KafkaJoinUserCountry> joinOutputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-join-stream-table-test");
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

        userInputTopic = testDriver.createInputTopic(
                USER_TOPIC,
                new StringSerializer(),
                SerdesUtils.<KafkaUser>getValueSerdes().serializer());
        countryInputTopic = testDriver.createInputTopic(
                COUNTRY_TOPIC,
                new StringSerializer(),
                SerdesUtils.<KafkaCountry>getValueSerdes().serializer());
        rekeyUserOutputTopic = testDriver.createOutputTopic(
                "streams-join-stream-table-test-" + USER_JOIN_STREAM_TABLE_REKEY_TOPIC + "-repartition",
                new StringDeserializer(),
                SerdesUtils.<KafkaUser>getValueSerdes().deserializer());
        joinOutputTopic = testDriver.createOutputTopic(
                USER_COUNTRY_JOIN_STREAM_TABLE_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<KafkaJoinUserCountry>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Path.of(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldRekey() {
        KafkaUser user = buildKafkaUser();
        userInputTopic.pipeInput("1", user);

        List<KeyValue<String, KafkaUser>> results = rekeyUserOutputTopic.readKeyValuesToList();

        assertEquals(KeyValue.pair("US", user), results.getFirst());
    }

    @Test
    void shouldJoinUserToCountry() {
        KafkaCountry country = buildKafkaCountry();
        countryInputTopic.pipeInput("US", country);

        KafkaUser user = buildKafkaUser();
        userInputTopic.pipeInput("1", user);

        List<KeyValue<String, KafkaJoinUserCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("US", results.getFirst().key);
        assertEquals(user, results.getFirst().value.getUser());
        assertEquals(country, results.getFirst().value.getCountry());
    }

    @Test
    void shouldNotJoinWhenNoCountry() {
        userInputTopic.pipeInput("1", buildKafkaUser());
        List<KeyValue<String, KafkaJoinUserCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertTrue(results.isEmpty());
    }

    private KafkaUser buildKafkaUser() {
        return KafkaUser.newBuilder()
                .setId(1L)
                .setFirstName("Homer")
                .setLastName("Simpson")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
                .setNationality(CountryCode.US)
                .build();
    }

    private KafkaCountry buildKafkaCountry() {
        return KafkaCountry.newBuilder()
                .setCode(CountryCode.US)
                .setName("United States")
                .setCapital("Washington")
                .setOfficialLanguage("English")
                .build();
    }
}
