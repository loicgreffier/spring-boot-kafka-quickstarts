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

package io.github.loicgreffier.streams.leftjoin.stream.globaltable;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.leftjoin.stream.globaltable.constant.Topic.COUNTRY_TOPIC;
import static io.github.loicgreffier.streams.leftjoin.stream.globaltable.constant.Topic.PERSON_COUNTRY_LEFT_JOIN_STREAM_GLOBAL_TABLE_TOPIC;
import static io.github.loicgreffier.streams.leftjoin.stream.globaltable.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinPersonCountry;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.leftjoin.stream.globaltable.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.leftjoin.stream.globaltable.serdes.SerdesUtils;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsLeftJoinStreamGtableApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsLeftJoinStreamGtableApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> personInputTopic;
    private TestInputTopic<String, KafkaCountry> countryInputTopic;
    private TestOutputTopic<String, KafkaJoinPersonCountry> joinOutputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-left-join-stream-global-table-test");
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

        personInputTopic = testDriver.createInputTopic(
            PERSON_TOPIC,
            new StringSerializer(),
            SerdesUtils.<KafkaPerson>getValueSerdes().serializer()
        );
        countryInputTopic = testDriver.createInputTopic(
            COUNTRY_TOPIC,
            new StringSerializer(),
            SerdesUtils.<KafkaCountry>getValueSerdes().serializer()
        );
        joinOutputTopic = testDriver.createOutputTopic(
            PERSON_COUNTRY_LEFT_JOIN_STREAM_GLOBAL_TABLE_TOPIC,
            new StringDeserializer(),
            SerdesUtils.<KafkaJoinPersonCountry>getValueSerdes().deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldJoinPersonToCountry() {
        KafkaCountry country = buildKafkaCountry();
        countryInputTopic.pipeInput("US", country);

        KafkaPerson person = buildKafkaPerson();
        personInputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaJoinPersonCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("1", results.getFirst().key);
        assertEquals(person, results.getFirst().value.getPerson());
        assertEquals(country, results.getFirst().value.getCountry());
    }

    @Test
    void shouldEmitValueEvenIfNoCountry() {
        KafkaPerson person = buildKafkaPerson();
        personInputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaJoinPersonCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("1", results.getFirst().key);
        assertEquals(person, results.getFirst().value.getPerson());
        assertNull(results.getFirst().value.getCountry());
    }

    private KafkaPerson buildKafkaPerson() {
        return KafkaPerson.newBuilder()
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
