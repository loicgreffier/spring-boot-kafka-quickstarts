package io.github.loicgreffier.streams.exception.handler.deserialization;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.exception.handler.deserialization.constant.Topic.PERSON_DESERIALIZATION_EXCEPTION_HANDLER_TOPIC;
import static io.github.loicgreffier.streams.exception.handler.deserialization.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.exception.handler.deserialization.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.exception.handler.deserialization.error.CustomDeserializationExceptionHandler;
import io.github.loicgreffier.streams.exception.handler.deserialization.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

class KafkaStreamsExceptionHandlerDeserializationApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsExceptionHandlerDeserializationApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestInputTopic<String, String> inputTopicForDeserializationException;
    private TestOutputTopic<String, KafkaPerson> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-deserialization-exception-handler-test");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
            CustomDeserializationExceptionHandler.class.getName());
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
        inputTopicForDeserializationException = testDriver.createInputTopic(
            PERSON_TOPIC,
            new StringSerializer(),
            new StringSerializer()
        );
        outputTopic = testDriver.createOutputTopic(
            PERSON_DESERIALIZATION_EXCEPTION_HANDLER_TOPIC,
            new StringDeserializer(),
            SerdesUtils.<KafkaPerson>getValueSerdes().deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldHandleDeserializationExceptionsAndContinueProcessing() {
        KafkaPerson homer = buildKafkaPerson("Homer");
        inputTopic.pipeInput("1", homer);

        inputTopicForDeserializationException.pipeInput("2", "invalid");

        KafkaPerson marge = buildKafkaPerson("Marge");
        inputTopic.pipeInput("3", marge);

        inputTopicForDeserializationException.pipeInput("4", "invalid");

        KafkaPerson bart = buildKafkaPerson("Bart");
        inputTopic.pipeInput("5", bart);

        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();

        assertEquals(KeyValue.pair("1", homer), results.get(0));
        assertEquals(KeyValue.pair("3", marge), results.get(1));
        assertEquals(KeyValue.pair("5", bart), results.get(2));

        final MetricName dropTotal = droppedRecordsTotalMetric();
        final MetricName dropRate = droppedRecordsRateMetric();

        assertEquals(2.0, testDriver.metrics().get(dropTotal).metricValue());
        assertEquals(0.06666666666666667, testDriver.metrics().get(dropRate).metricValue());
    }

    private KafkaPerson buildKafkaPerson(String firstName) {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName(firstName)
            .setLastName("Simpson")
            .setNationality(CountryCode.US)
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build();
    }

    private MetricName droppedRecordsTotalMetric() {
        return new MetricName(
            "dropped-records-total",
            "stream-task-metrics",
            "The total number of dropped records",
            mkMap(
                mkEntry("thread-id", Thread.currentThread().getName()),
                mkEntry("task-id", "0_0")
            )
        );
    }

    private MetricName droppedRecordsRateMetric() {
        return new MetricName(
            "dropped-records-rate",
            "stream-task-metrics",
            "The average number of dropped records per second",
            mkMap(
                mkEntry("thread-id", Thread.currentThread().getName()),
                mkEntry("task-id", "0_0")
            )
        );
    }
}
