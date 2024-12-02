package io.github.loicgreffier.streams.exception.handler.processing;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.exception.handler.processing.constant.Topic.PERSON_PROCESSING_EXCEPTION_HANDLER_TOPIC;
import static io.github.loicgreffier.streams.exception.handler.processing.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.exception.handler.processing.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.exception.handler.processing.error.CustomProcessingExceptionHandler;
import io.github.loicgreffier.streams.exception.handler.processing.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
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
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPerson> outputTopic;

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

        // Create SerDes that throws an exception when the application tries to serialize
        Map<String, String> serializationExceptionConfig = Map.of(
            SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL,
            AUTO_REGISTER_SCHEMAS, "false"
        );
        SerdesUtils.setSerdesConfig(serializationExceptionConfig);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(
            streamsBuilder.build(),
            properties,
            Instant.parse("2000-01-01T01:00:00Z")
        );

        // Create SerDes for input and output topics only
        Map<String, String> config = Map.of(
            SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL
        );

        SpecificAvroSerde<KafkaPerson> serDes = new SpecificAvroSerde<>();
        serDes.configure(config, false);

        inputTopic = testDriver.createInputTopic(
            PERSON_TOPIC,
            new StringSerializer(),
            serDes.serializer()
        );
        outputTopic = testDriver.createOutputTopic(
            PERSON_PROCESSING_EXCEPTION_HANDLER_TOPIC,
            new StringDeserializer(),
            serDes.deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldHandleProcessingExceptionsFromDslOperation() {
        inputTopic.pipeInput("1", buildKafkaPerson("Homer", Instant.parse("1969-01-01T01:00:00Z")));

        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();

        assertTrue(results.isEmpty());

        final MetricName dropTotal = droppedRecordsTotalMetric();
        final MetricName dropRate = droppedRecordsRateMetric();

        assertEquals(1.0, testDriver.metrics().get(dropTotal).metricValue());
        assertEquals(0.03333333333333333, testDriver.metrics().get(dropRate).metricValue());
    }

    @Test
    void shouldHandleProcessingExceptionsFromProcessor() {
        inputTopic.pipeInput("1", buildKafkaPerson(null, Instant.parse("1980-01-01T01:00:00Z")));

        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();

        assertTrue(results.isEmpty());

        final MetricName dropTotal = droppedRecordsTotalMetric();
        final MetricName dropRate = droppedRecordsRateMetric();

        assertEquals(1.0, testDriver.metrics().get(dropTotal).metricValue());
        assertEquals(0.03333333333333333, testDriver.metrics().get(dropRate).metricValue());
    }

    @Test
    void shouldHandleProcessingExceptionsFromPunctuation() {
        testDriver.advanceWallClockTime(Duration.ofMinutes(2));

        final MetricName dropTotal = droppedRecordsTotalMetric();
        final MetricName dropRate = droppedRecordsRateMetric();

        assertEquals(1.0, testDriver.metrics().get(dropTotal).metricValue());
        assertEquals(0.03333333333333333, testDriver.metrics().get(dropRate).metricValue());
    }

    private KafkaPerson buildKafkaPerson(String firstName, Instant birthDate) {
        return KafkaPerson.newBuilder()
            .setId(10L)
            .setFirstName(firstName)
            .setLastName("Simpson")
            .setNationality(CountryCode.US)
            .setBirthDate(birthDate)
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

    /**
     * Mock Serde that throws an exception when serializing.
     *
     * @param <T> the type of the record.
     */
    public static class SerdeMock<T extends org.apache.avro.specific.SpecificRecord> implements Serde<T> {
        private final KafkaAvroDeserializer kafkaAvroDeserializer;

        public SerdeMock() {
            kafkaAvroDeserializer = new KafkaAvroDeserializer();
        }

        @Override
        public Serializer<T> serializer() {
            return (topic, data) -> {
                throw new RuntimeException("Error while serializing");
            };
        }

        @Override
        @SuppressWarnings("unchecked")
        public Deserializer<T> deserializer() {
            return (topic, data) -> (T) kafkaAvroDeserializer.deserialize(topic, data);
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            Map<String, Object> specificAvroEnabledConfig = configs == null ? new HashMap<>() : new HashMap<>(configs);
            specificAvroEnabledConfig.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
            kafkaAvroDeserializer.configure(specificAvroEnabledConfig, isKey);
        }
    }
}