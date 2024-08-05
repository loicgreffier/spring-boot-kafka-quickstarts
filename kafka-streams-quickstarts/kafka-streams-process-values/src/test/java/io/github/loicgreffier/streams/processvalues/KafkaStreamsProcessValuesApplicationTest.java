package io.github.loicgreffier.streams.processvalues;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.processvalues.constant.Topic.PERSON_PROCESS_VALUES_TOPIC;
import static io.github.loicgreffier.streams.processvalues.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import io.github.loicgreffier.streams.processvalues.app.KafkaStreamsTopology;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
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

/**
 * This class contains unit tests for the {@link KafkaStreamsTopology} class.
 */
class KafkaStreamsProcessValuesApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsProcessValuesApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPersonMetadata> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-process-values-test");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(
            streamsBuilder.build(),
            properties,
            Instant.parse("2000-01-01T01:00:00Z")
        );

        // Create Serde for input and output topics
        Serde<KafkaPerson> personSerde = new SpecificAvroSerde<>();
        Serde<KafkaPersonMetadata> personMetadataSerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        personMetadataSerde.configure(config, false);

        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(
            PERSON_PROCESS_VALUES_TOPIC,
            new StringDeserializer(),
            personMetadataSerde.deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldProcessValues() {
        KafkaPerson firstPersonDoe = buildKafkaPerson("John", "Doe");
        KafkaPerson secondPersonDoe = buildKafkaPerson("Michael", "Doe");
        KafkaPerson firstPersonSmith = buildKafkaPerson("Jane", "Smith");
        KafkaPerson secondPersonSmith = buildKafkaPerson("Daniel", "Smith");

        inputTopic.pipeInput("1", firstPersonDoe);
        inputTopic.pipeInput("2", secondPersonDoe);
        inputTopic.pipeInput("3", firstPersonSmith);
        inputTopic.pipeInput("4", secondPersonSmith);

        List<KeyValue<String, KafkaPersonMetadata>> results = outputTopic.readKeyValuesToList();

        assertEquals(firstPersonDoe, results.get(0).value.getPerson());
        assertEquals(PERSON_TOPIC, results.get(0).value.getTopic());
        assertEquals(0, results.get(0).value.getPartition());
        assertEquals(0, results.get(0).value.getOffset());

        assertEquals(secondPersonDoe, results.get(1).value.getPerson());
        assertEquals(PERSON_TOPIC, results.get(1).value.getTopic());
        assertEquals(0, results.get(1).value.getPartition());
        assertEquals(1, results.get(1).value.getOffset());

        assertEquals(firstPersonSmith, results.get(2).value.getPerson());
        assertEquals(PERSON_TOPIC, results.get(2).value.getTopic());
        assertEquals(0, results.get(2).value.getPartition());
        assertEquals(2, results.get(2).value.getOffset());

        assertEquals(secondPersonSmith, results.get(3).value.getPerson());
        assertEquals(PERSON_TOPIC, results.get(3).value.getTopic());
        assertEquals(0, results.get(3).value.getPartition());
        assertEquals(3, results.get(3).value.getOffset());
    }

    private KafkaPerson buildKafkaPerson(String firstName, String lastName) {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName(firstName)
            .setLastName(lastName)
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build();
    }
}
