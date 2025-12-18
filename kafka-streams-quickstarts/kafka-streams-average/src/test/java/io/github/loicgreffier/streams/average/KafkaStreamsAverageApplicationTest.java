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
package io.github.loicgreffier.streams.average;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.average.constant.StateStore.USER_AVERAGE_STORE;
import static io.github.loicgreffier.streams.average.constant.Topic.USER_AVERAGE_TOPIC;
import static io.github.loicgreffier.streams.average.constant.Topic.USER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.avro.KafkaUserAverageAge;
import io.github.loicgreffier.streams.average.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.average.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsAverageApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsAverageApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaUser> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-average-test");
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
                testDriver.createOutputTopic(USER_AVERAGE_TOPIC, new StringDeserializer(), new LongDeserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Path.of(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldComputeAverageAgeByNationality() {
        LocalDate currentDate = LocalDate.now();

        KafkaUser yearsOld25 = buildKafkaUser(
                "Homer", currentDate.minusYears(25).atStartOfDay().toInstant(ZoneOffset.UTC));
        KafkaUser yearsOld75 = buildKafkaUser(
                "Marge", currentDate.minusYears(75).atStartOfDay().toInstant(ZoneOffset.UTC));

        inputTopic.pipeInput("1", yearsOld25);
        inputTopic.pipeInput("2", yearsOld75);

        List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

        assertEquals(KeyValue.pair("US", 25L), results.get(0));
        assertEquals(KeyValue.pair("US", 50L), results.get(1));

        KeyValueStore<String, KafkaUserAverageAge> stateStore = testDriver.getKeyValueStore(USER_AVERAGE_STORE);

        assertEquals(2L, stateStore.get("US").getCount());
        assertEquals(100L, stateStore.get("US").getAgeSum());
    }

    private KafkaUser buildKafkaUser(String firstName, Instant birthDate) {
        return KafkaUser.newBuilder()
                .setId(1L)
                .setFirstName(firstName)
                .setLastName("Simpson")
                .setNationality(CountryCode.US)
                .setBirthDate(birthDate)
                .build();
    }
}
