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
package io.github.loicgreffier.streams.branch;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.branch.constant.Topic.ORDER_BRANCH_A_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.ORDER_BRANCH_B_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.ORDER_BRANCH_DEFAULT_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.ORDER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.streams.branch.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.branch.serdes.SerdesUtils;
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

class KafkaStreamsBranchApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsBranchApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> inputTopic;
    private TestOutputTopic<String, Order> outputTopicA;
    private TestOutputTopic<String, Order> outputTopicB;
    private TestOutputTopic<String, Order> outputTopicDefault;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-branch-test");
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
        outputTopicA = testDriver.createOutputTopic(
                ORDER_BRANCH_A_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<Order>getValueSerdes().deserializer());
        outputTopicB = testDriver.createOutputTopic(
                ORDER_BRANCH_B_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<Order>getValueSerdes().deserializer());
        outputTopicDefault = testDriver.createOutputTopic(
                ORDER_BRANCH_DEFAULT_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<Order>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Path.of(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldBranchToTopicA() {
        inputTopic.pipeInput("1", buildOrder(1L, 3L, List.of("Laptop", "Mouse"), 1500.0));

        List<KeyValue<String, Order>> results = outputTopicA.readKeyValuesToList();

        assertEquals(1350.0, results.getFirst().value.getAmount());
    }

    @Test
    void shouldBranchToTopicB() {
        Order order = buildOrder(1L, 3L, List.of("Keyboard"), 500.0);
        inputTopic.pipeInput("1", order);

        List<KeyValue<String, Order>> results = outputTopicB.readKeyValuesToList();

        assertEquals(KeyValue.pair("1", order), results.getFirst());
    }

    @Test
    void shouldBranchToDefaultTopic() {
        Order order = buildOrder(1L, 3L, List.of("Mouse Pad"), 50.0);
        inputTopic.pipeInput("1", order);

        List<KeyValue<String, Order>> results = outputTopicDefault.readKeyValuesToList();

        assertEquals(KeyValue.pair("1", order), results.getFirst());
    }

    private Order buildOrder(long id, long customerId, List<String> items, double amount) {
        return Order.newBuilder()
                .setId(id)
                .setCustomerId(customerId)
                .setItems(items)
                .setAmount(amount)
                .build();
    }
}
