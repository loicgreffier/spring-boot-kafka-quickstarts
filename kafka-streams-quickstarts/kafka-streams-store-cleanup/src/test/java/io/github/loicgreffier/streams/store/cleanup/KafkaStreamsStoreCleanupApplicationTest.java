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
import static io.github.loicgreffier.streams.store.cleanup.constant.StateStore.ORDER_STORE_CLEANUP_STORE;
import static io.github.loicgreffier.streams.store.cleanup.constant.Topic.ORDER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.streams.store.cleanup.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.store.cleanup.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsStoreCleanupApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsStoreCleanupApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> inputTopic;

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
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00Z"));

        inputTopic = testDriver.createInputTopic(
                ORDER_TOPIC,
                new StringSerializer(),
                SerdesUtils.<Order>getValueSerdes().serializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Path.of(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldFillAndCleanupStore() {
        Order orderOne = buildOrder("ORD1");
        inputTopic.pipeInput("ORD1", orderOne, Instant.parse("2000-01-01T01:00:00Z"));

        Order orderTwo = buildOrder("ORD2");
        inputTopic.pipeInput("ORD2", orderTwo, Instant.parse("2000-01-01T01:00:20Z"));

        Order orderThree = buildOrder("ORD3");
        inputTopic.pipeInput("ORD3", orderThree, Instant.parse("2000-01-01T01:00:40Z"));

        KeyValueStore<String, Order> store = testDriver.getKeyValueStore(ORDER_STORE_CLEANUP_STORE);

        // The 1st stream time punctuate is triggered after the 1st record is pushed,
        // so the 1st record is not in the store anymore.
        assertNull(store.get("ORD1"));
        assertEquals(orderTwo, store.get("ORD2"));
        assertEquals(orderThree, store.get("ORD3"));

        Order orderFour = buildOrder("ORD4");
        inputTopic.pipeInput("ORD4", orderFour, Instant.parse("2000-01-01T01:02:00Z"));

        Order orderFive = buildOrder("ORD5");
        inputTopic.pipeInput("ORD5", orderFive, Instant.parse("2000-01-01T01:02:30Z"));

        // 2nd stream time punctuate
        assertNull(store.get("ORD2"));
        assertNull(store.get("ORD3"));
        assertNull(store.get("ORD4"));
        assertEquals(orderFive, store.get("ORD5"));
    }

    private Order buildOrder(String id) {
        return Order.newBuilder()
                .setId(id)
                .setCustomerId("CUS1")
                .setItems(List.of("Laptop", "Mouse"))
                .setAmount(1249.90)
                .build();
    }
}
