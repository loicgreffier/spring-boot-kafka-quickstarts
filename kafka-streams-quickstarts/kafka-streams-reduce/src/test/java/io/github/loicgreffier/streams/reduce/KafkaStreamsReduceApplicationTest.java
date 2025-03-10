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
package io.github.loicgreffier.streams.reduce;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.reduce.constant.StateStore.USER_REDUCE_STORE;
import static io.github.loicgreffier.streams.reduce.constant.Topic.USER_REDUCE_TOPIC;
import static io.github.loicgreffier.streams.reduce.constant.Topic.USER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.reduce.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.reduce.serdes.SerdesUtils;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsReduceApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsReduceApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaUser> inputTopic;
    private TestOutputTopic<String, KafkaUser> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-reduce-test");
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
                USER_REDUCE_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<KafkaUser>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldReduceByNationalityAndKeepOldest() {
        KafkaUser oldestUs = buildKafkaUser("Homer", "Simpson", Instant.parse("1956-08-29T18:35:24Z"), CountryCode.US);

        KafkaUser youngestUs = buildKafkaUser("Bart", "Simpson", Instant.parse("1994-11-09T08:08:50Z"), CountryCode.US);

        KafkaUser youngestBe =
                buildKafkaUser("Milhouse", "Van Houten", Instant.parse("1996-02-02T04:58:01Z"), CountryCode.BE);

        KafkaUser oldestBe =
                buildKafkaUser("Kirk", "Van Houten", Instant.parse("1976-05-26T04:52:06Z"), CountryCode.BE);

        inputTopic.pipeInput("1", oldestUs);
        inputTopic.pipeInput("2", youngestUs);
        inputTopic.pipeInput("3", youngestBe);
        inputTopic.pipeInput("4", oldestBe);

        List<KeyValue<String, KafkaUser>> results = outputTopic.readKeyValuesToList();

        assertEquals(KeyValue.pair(CountryCode.US.toString(), oldestUs), results.get(0));
        assertEquals(KeyValue.pair(CountryCode.US.toString(), oldestUs), results.get(1));
        assertEquals(KeyValue.pair(CountryCode.BE.toString(), youngestBe), results.get(2));
        assertEquals(KeyValue.pair(CountryCode.BE.toString(), oldestBe), results.get(3));

        KeyValueStore<String, KafkaUser> stateStore = testDriver.getKeyValueStore(USER_REDUCE_STORE);

        assertEquals(oldestUs, stateStore.get(CountryCode.US.toString()));
        assertEquals(oldestBe, stateStore.get(CountryCode.BE.toString()));
    }

    private KafkaUser buildKafkaUser(String firstName, String lastName, Instant birthDate, CountryCode nationality) {
        return KafkaUser.newBuilder()
                .setId(1L)
                .setFirstName(firstName)
                .setLastName(lastName)
                .setBirthDate(birthDate)
                .setNationality(nationality)
                .build();
    }
}
