package io.github.loicgreffier.streams.branch;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.branch.constant.Topic.PERSON_BRANCH_A_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.PERSON_BRANCH_B_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.PERSON_BRANCH_DEFAULT_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.branch.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.branch.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPerson> outputTopicA;
    private TestOutputTopic<String, KafkaPerson> outputTopicB;
    private TestOutputTopic<String, KafkaPerson> outputTopicDefault;

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
        outputTopicA = testDriver.createOutputTopic(
            PERSON_BRANCH_A_TOPIC,
            new StringDeserializer(),
            SerdesUtils.<KafkaPerson>getValueSerdes().deserializer()
        );
        outputTopicB = testDriver.createOutputTopic(
            PERSON_BRANCH_B_TOPIC,
            new StringDeserializer(),
            SerdesUtils.<KafkaPerson>getValueSerdes().deserializer()
        );
        outputTopicDefault = testDriver.createOutputTopic(
            PERSON_BRANCH_DEFAULT_TOPIC,
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
    void shouldBranchToTopicA() {
        inputTopic.pipeInput("1", buildKafkaPerson("Homer", "Simpson"));

        List<KeyValue<String, KafkaPerson>> results = outputTopicA.readKeyValuesToList();

        assertEquals("HOMER", results.getFirst().value.getFirstName());
        assertEquals("SIMPSON", results.getFirst().value.getLastName());
    }

    @Test
    void shouldBranchToTopicB() {
        KafkaPerson person = buildKafkaPerson("Ned", "Flanders");
        inputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaPerson>> results = outputTopicB.readKeyValuesToList();

        assertEquals(KeyValue.pair("1", person), results.getFirst());
    }

    @Test
    void shouldBranchToDefaultTopic() {
        KafkaPerson person = buildKafkaPerson("Milhouse", "Van Houten");
        inputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaPerson>> results = outputTopicDefault.readKeyValuesToList();

        assertEquals(KeyValue.pair("1", person), results.getFirst());
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
