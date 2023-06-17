package io.github.loicgreffier.streams.processvalues;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import io.github.loicgreffier.streams.processvalues.app.KafkaStreamsProcessValuesTopology;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.github.loicgreffier.streams.processvalues.constants.Topic.PERSON_PROCESS_VALUES_TOPIC;
import static io.github.loicgreffier.streams.processvalues.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsProcessValuesTest {
    private static final String SCHEMA_REGISTRY_SCOPE = KafkaStreamsProcessValuesTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPersonMetadata> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-process-values-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsProcessValuesTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        // Create Serde for input and output topics
        Serde<KafkaPerson> personSerde = new SpecificAvroSerde<>();
        Serde<KafkaPersonMetadata> personMetadataSerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        personMetadataSerde.configure(config, false);
        
        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(PERSON_PROCESS_VALUES_TOPIC, new StringDeserializer(), personMetadataSerde.deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldProcessValues() {
        List<KeyValue<String, KafkaPerson>> persons = buildKafkaPersonRecords();
        inputTopic.pipeKeyValueList(persons);
        List<KeyValue<String, KafkaPersonMetadata>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(4);
        assertThat(results.get(0).value.getPerson()).isEqualTo(persons.get(0).value);
        assertThat(results.get(0).value.getTopic()).isEqualTo(PERSON_TOPIC);
        assertThat(results.get(0).value.getPartition()).isZero();
        assertThat(results.get(0).value.getOffset()).isZero();

        assertThat(results.get(1).value.getPerson()).isEqualTo(persons.get(1).value);
        assertThat(results.get(1).value.getTopic()).isEqualTo(PERSON_TOPIC);
        assertThat(results.get(1).value.getPartition()).isZero();
        assertThat(results.get(1).value.getOffset()).isEqualTo(1);

        assertThat(results.get(2).value.getPerson()).isEqualTo(persons.get(2).value);
        assertThat(results.get(2).value.getTopic()).isEqualTo(PERSON_TOPIC);
        assertThat(results.get(2).value.getPartition()).isZero();
        assertThat(results.get(2).value.getOffset()).isEqualTo(2);

        assertThat(results.get(3).value.getPerson()).isEqualTo(persons.get(3).value);
        assertThat(results.get(3).value.getTopic()).isEqualTo(PERSON_TOPIC);
        assertThat(results.get(3).value.getPartition()).isZero();
        assertThat(results.get(3).value.getOffset()).isEqualTo(3);
    }

    private List<KeyValue<String, KafkaPerson>> buildKafkaPersonRecords() {
        return List.of(
                KeyValue.pair("1", KafkaPerson.newBuilder().setId(1L).setFirstName("Aaran").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("2", KafkaPerson.newBuilder().setId(2L).setFirstName("Brendan").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("3", KafkaPerson.newBuilder().setId(3L).setFirstName("Bret").setLastName("Holman").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("4", KafkaPerson.newBuilder().setId(3L).setFirstName("Jude").setLastName("Holman").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build())
        );
    }
}
