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

package io.github.loicgreffier.streams.exception.handler.processing;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.exception.handler.processing.constant.Topic.USER_PROCESSING_EXCEPTION_HANDLER_TOPIC;
import static io.github.loicgreffier.streams.exception.handler.processing.constant.Topic.USER_TOPIC;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.exception.handler.processing.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.exception.handler.processing.error.CustomProcessingExceptionHandler;
import io.github.loicgreffier.streams.exception.handler.processing.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.MetricName;
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

class KafkaStreamsExceptionHandlerProcessingApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsExceptionHandlerProcessingApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaUser> inputTopic;
    private TestOutputTopic<String, KafkaUser> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-processing-exception-handler-test");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG,
            CustomProcessingExceptionHandler.class.getName());
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
            USER_TOPIC,
            new StringSerializer(),
            SerdesUtils.<KafkaUser>getValueSerdes().serializer()
        );
        outputTopic = testDriver.createOutputTopic(
            USER_PROCESSING_EXCEPTION_HANDLER_TOPIC,
            new StringDeserializer(),
            SerdesUtils.<KafkaUser>getValueSerdes().deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldHandleIllegalArgumentExceptionAndContinueProcessing() {
        inputTopic.pipeInput("1", buildKafkaUser("Homer", Instant.parse("1949-01-01T01:00:00Z")));
        inputTopic.pipeInput("2", buildKafkaUser(null, Instant.parse("1980-01-01T01:00:00Z")));

        testDriver.advanceWallClockTime(Duration.ofMinutes(2));

        List<KeyValue<String, KafkaUser>> results = outputTopic.readKeyValuesToList();

        assertTrue(results.isEmpty());

        assertEquals(3.0, testDriver.metrics().get(droppedRecordsTotalMetric()).metricValue());
        assertEquals(0.03333333333333333, testDriver.metrics().get(droppedRecordsRateMetric()).metricValue());
    }

    private KafkaUser buildKafkaUser(String firstName, Instant birthDate) {
        return KafkaUser.newBuilder()
            .setId(10L)
            .setFirstName(firstName)
            .setLastName("Simpson")
            .setNationality(CountryCode.US)
            .setBirthDate(birthDate)
            .build();
    }

    private MetricName droppedRecordsTotalMetric() {
        return createMetric("dropped-records-total", "The total number of dropped records");
    }

    private MetricName droppedRecordsRateMetric() {
        return createMetric("dropped-records-rate", "The average number of dropped records per second");
    }

    private MetricName createMetric(String name, String description) {
        return new MetricName(
            name, "stream-task-metrics", description,
            mkMap(
                mkEntry("thread-id", Thread.currentThread().getName()),
                mkEntry("task-id", "0_0")
            )
        );
    }
}
