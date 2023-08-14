package io.github.loicgreffier.streams.branch;

import static io.github.loicgreffier.streams.branch.constants.Topic.PERSON_BRANCH_A_TOPIC;
import static io.github.loicgreffier.streams.branch.constants.Topic.PERSON_BRANCH_B_TOPIC;
import static io.github.loicgreffier.streams.branch.constants.Topic.PERSON_BRANCH_DEFAULT_TOPIC;
import static io.github.loicgreffier.streams.branch.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.branch.app.KafkaStreamsTopology;
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
 * This class contains integration tests for the {@link KafkaStreamsTopology} class.
 */
class KafkaStreamsBranchApplicationTests {
    private static final String SCHEMA_REGISTRY_SCOPE =
        KafkaStreamsBranchApplicationTests.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPerson> outputTopicA;
    private TestOutputTopic<String, KafkaPerson> outputTopicB;
    private TestOutputTopic<String, KafkaPerson> outputTopicDefault;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-branch-test");
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
        Map<String, String> config =
            Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);

        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(),
            personSerde.serializer());
        outputTopicA = testDriver.createOutputTopic(PERSON_BRANCH_A_TOPIC, new StringDeserializer(),
            personSerde.deserializer());
        outputTopicB = testDriver.createOutputTopic(PERSON_BRANCH_B_TOPIC, new StringDeserializer(),
            personSerde.deserializer());
        outputTopicDefault =
            testDriver.createOutputTopic(PERSON_BRANCH_DEFAULT_TOPIC, new StringDeserializer(),
                personSerde.deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldBranchToTopicA() {
        inputTopic.pipeInput("1", buildKafkaPerson("Alex", "Adams"));

        List<KeyValue<String, KafkaPerson>> results = outputTopicA.readKeyValuesToList();

        assertThat(results.get(0).value.getFirstName()).isEqualTo("ALEX");
        assertThat(results.get(0).value.getLastName()).isEqualTo("ADAMS");
    }

    @Test
    void shouldBranchToTopicB() {
        KafkaPerson person = buildKafkaPerson("Ben", "Brown");
        inputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaPerson>> results = outputTopicB.readKeyValuesToList();

        assertThat(results.get(0)).isEqualTo(KeyValue.pair("1", person));
    }

    @Test
    void shouldBranchToDefaultTopic() {
        KafkaPerson person = buildKafkaPerson("John", "Doe");
        inputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaPerson>> results = outputTopicDefault.readKeyValuesToList();

        assertThat(results.get(0)).isEqualTo(KeyValue.pair("1", person));
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
