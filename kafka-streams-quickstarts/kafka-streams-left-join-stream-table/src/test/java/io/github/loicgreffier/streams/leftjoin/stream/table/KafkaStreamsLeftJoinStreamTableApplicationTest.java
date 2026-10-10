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
package io.github.loicgreffier.streams.leftjoin.stream.table;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.leftjoin.stream.table.constant.Topic.CUSTOMER_TOPIC;
import static io.github.loicgreffier.streams.leftjoin.stream.table.constant.Topic.ORDER_CUSTOMER_LEFT_JOIN_STREAM_TABLE_TOPIC;
import static io.github.loicgreffier.streams.leftjoin.stream.table.constant.Topic.ORDER_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC;
import static io.github.loicgreffier.streams.leftjoin.stream.table.constant.Topic.ORDER_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.Customer;
import io.github.loicgreffier.avro.JoinOrderCustomer;
import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.streams.leftjoin.stream.table.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.leftjoin.stream.table.serdes.SerdesUtils;
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

class KafkaStreamsLeftJoinStreamTableApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsLeftJoinStreamTableApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Order> orderInputTopic;
    private TestInputTopic<String, Customer> customerInputTopic;
    private TestOutputTopic<String, Order> rekeyOrderOutputTopic;
    private TestOutputTopic<String, JoinOrderCustomer> joinOutputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-left-join-stream-table-test");
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

        orderInputTopic = testDriver.createInputTopic(
                ORDER_TOPIC,
                new StringSerializer(),
                SerdesUtils.<Order>getValueSerdes().serializer());
        customerInputTopic = testDriver.createInputTopic(
                CUSTOMER_TOPIC,
                new StringSerializer(),
                SerdesUtils.<Customer>getValueSerdes().serializer());
        rekeyOrderOutputTopic = testDriver.createOutputTopic(
                "streams-left-join-stream-table-test-" + ORDER_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC + "-repartition",
                new StringDeserializer(),
                SerdesUtils.<Order>getValueSerdes().deserializer());
        joinOutputTopic = testDriver.createOutputTopic(
                ORDER_CUSTOMER_LEFT_JOIN_STREAM_TABLE_TOPIC,
                new StringDeserializer(),
                SerdesUtils.<JoinOrderCustomer>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Path.of(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldRekey() {
        Order order = buildOrder();
        orderInputTopic.pipeInput("1", order);

        List<KeyValue<String, Order>> results = rekeyOrderOutputTopic.readKeyValuesToList();

        assertEquals(KeyValue.pair("3", order), results.getFirst());
    }

    @Test
    void shouldJoinOrderToCustomer() {
        Customer customer = buildCustomer();
        customerInputTopic.pipeInput("3", customer);

        Order order = buildOrder();
        orderInputTopic.pipeInput("1", order);

        List<KeyValue<String, JoinOrderCustomer>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("3", results.getFirst().key);
        assertEquals(order, results.getFirst().value.getOrder());
        assertEquals(customer, results.getFirst().value.getCustomer());
    }

    @Test
    void shouldEmitValueEvenIfNoCustomer() {
        Order order = buildOrder();
        orderInputTopic.pipeInput("1", order);

        List<KeyValue<String, JoinOrderCustomer>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("3", results.getFirst().key);
        assertEquals(order, results.getFirst().value.getOrder());
        assertNull(results.getFirst().value.getCustomer());
    }

    private Order buildOrder() {
        return Order.newBuilder()
                .setId(1L)
                .setCustomerId(3L)
                .setItems(List.of("Laptop", "Mouse"))
                .setAmount(1249.90)
                .build();
    }

    private Customer buildCustomer() {
        return Customer.newBuilder()
                .setId(3L)
                .setFirstName("Homer")
                .setLastName("Simpson")
                .setEmail("homer.simpson@mail.com")
                .build();
    }
}
