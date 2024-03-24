package io.github.loicgreffier.streams.process;

import static io.github.loicgreffier.streams.process.constant.Topic.PERSON_PROCESS_TOPIC;
import static io.github.loicgreffier.streams.process.constant.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import io.github.loicgreffier.streams.process.app.KafkaStreamsTopology;
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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class contains unit tests for the {@link KafkaStreamsTopology} class.
 */
class KafkaStreamsProcessApplicationTests {
    private static final String SCHEMA_REGISTRY_SCOPE =
        KafkaStreamsProcessApplicationTests.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPersonMetadata> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-process-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            SpecificAvroSerde.class.getName());
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            MOCK_SCHEMA_REGISTRY_URL);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties,
            Instant.parse("2000-01-01T01:00:00.00Z"));

        // Create Serde for input and output topics
        Serde<KafkaPerson> personSerde = new SpecificAvroSerde<>();
        Serde<KafkaPersonMetadata> personMetadataSerde = new SpecificAvroSerde<>();
        Map<String, String> config =
            Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        personMetadataSerde.configure(config, false);

        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(),
            personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(PERSON_PROCESS_TOPIC, new StringDeserializer(),
            personMetadataSerde.deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldProcess() {
        KafkaPerson firstPersonDoe = buildKafkaPerson("John", "Doe");
        KafkaPerson secondPersonDoe = buildKafkaPerson("Michael", "Doe");
        KafkaPerson firstPersonSmith = buildKafkaPerson("Jane", "Smith");
        KafkaPerson secondPersonSmith = buildKafkaPerson("Daniel", "Smith");

        inputTopic.pipeInput("1", firstPersonDoe);
        inputTopic.pipeInput("2", secondPersonDoe);
        inputTopic.pipeInput("3", firstPersonSmith);
        inputTopic.pipeInput("4", secondPersonSmith);

        List<KeyValue<String, KafkaPersonMetadata>> results = outputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Doe");
        assertThat(results.get(0).value.getPerson()).isEqualTo(firstPersonDoe);
        assertThat(results.get(0).value.getTopic()).isEqualTo(PERSON_TOPIC);
        assertThat(results.get(0).value.getPartition()).isZero();
        assertThat(results.get(0).value.getOffset()).isZero();

        assertThat(results.get(1).key).isEqualTo("Doe");
        assertThat(results.get(1).value.getPerson()).isEqualTo(secondPersonDoe);
        assertThat(results.get(1).value.getTopic()).isEqualTo(PERSON_TOPIC);
        assertThat(results.get(1).value.getPartition()).isZero();
        assertThat(results.get(1).value.getOffset()).isEqualTo(1);

        assertThat(results.get(2).key).isEqualTo("Smith");
        assertThat(results.get(2).value.getPerson()).isEqualTo(firstPersonSmith);
        assertThat(results.get(2).value.getTopic()).isEqualTo(PERSON_TOPIC);
        assertThat(results.get(2).value.getPartition()).isZero();
        assertThat(results.get(2).value.getOffset()).isEqualTo(2);

        assertThat(results.get(3).key).isEqualTo("Smith");
        assertThat(results.get(3).value.getPerson()).isEqualTo(secondPersonSmith);
        assertThat(results.get(3).value.getTopic()).isEqualTo(PERSON_TOPIC);
        assertThat(results.get(3).value.getPartition()).isZero();
        assertThat(results.get(3).value.getOffset()).isEqualTo(3);
    }

    private KafkaPerson buildKafkaPerson(String firstName, String lastName) {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName(firstName)
            .setLastName(lastName)
            .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
            .build();
    }
}
