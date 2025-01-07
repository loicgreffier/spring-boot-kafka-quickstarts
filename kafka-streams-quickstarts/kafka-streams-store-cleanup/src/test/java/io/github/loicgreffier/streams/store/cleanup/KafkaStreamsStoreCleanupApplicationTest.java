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

package io.github.loicgreffier.streams.store.cleanup;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.store.cleanup.constant.StateStore.PERSON_SCHEDULE_STORE_CLEANUP_STORE;
import static io.github.loicgreffier.streams.store.cleanup.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.store.cleanup.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.store.cleanup.serdes.SerdesUtils;
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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsStoreCleanupApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsStoreCleanupApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-store-cleanup-test");
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
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldFillAndCleanupStore() {
        KafkaPerson homer = buildKafkaPerson("Homer", "Simpson");
        inputTopic.pipeInput(new TestRecord<>("1", homer, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaPerson marge = buildKafkaPerson("Marge", "Simpson");
        inputTopic.pipeInput(new TestRecord<>("2", marge, Instant.parse("2000-01-01T01:00:20Z")));

        KafkaPerson milhouse = buildKafkaPerson("Milhouse", "Van Houten");
        inputTopic.pipeInput(new TestRecord<>("3", milhouse, Instant.parse("2000-01-01T01:00:40Z")));

        KeyValueStore<String, KafkaPerson> stateStore = testDriver
            .getKeyValueStore(PERSON_SCHEDULE_STORE_CLEANUP_STORE);

        // The 1st stream time punctuate is triggered after the 1st record is pushed, 
        // so the 1st record is not in the store anymore.
        assertNull(stateStore.get("1"));
        assertEquals(marge, stateStore.get("2"));
        assertEquals(milhouse, stateStore.get("3"));

        KafkaPerson bart = buildKafkaPerson("Bart", "Simpson");
        inputTopic.pipeInput(new TestRecord<>("4", bart, Instant.parse("2000-01-01T01:02:00Z")));

        KafkaPerson lisa = buildKafkaPerson("Lisa", "Simpson");
        inputTopic.pipeInput(new TestRecord<>("5", lisa, Instant.parse("2000-01-01T01:02:30Z")));

        // 2nd stream time punctuate
        assertNull(stateStore.get("2"));
        assertNull(stateStore.get("3"));
        assertNull(stateStore.get("4"));
        assertEquals(lisa, stateStore.get("5"));
    }

    private KafkaPerson buildKafkaPerson(String firstName, String lastName) {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName(firstName)
            .setLastName(lastName)
            .setNationality(CountryCode.GB)
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build();
    }
}
