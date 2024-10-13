package io.github.loicgreffier.streams.cogroup;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.cogroup.constant.StateStore.PERSON_COGROUP_AGGREGATE_STORE;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.PERSON_COGROUP_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.PERSON_TOPIC_TWO;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.cogroup.app.KafkaStreamsTopology;
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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsCogroupApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsCogroupApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopicOne;
    private TestInputTopic<String, KafkaPerson> inputTopicTwo;
    private TestOutputTopic<String, KafkaPersonGroup> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-cogroup-test");
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
        Serde<KafkaPersonGroup> personGroupSerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        personGroupSerde.configure(config, false);

        inputTopicOne = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        inputTopicTwo = testDriver.createInputTopic(PERSON_TOPIC_TWO, new StringSerializer(), personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(
            PERSON_COGROUP_TOPIC,
            new StringDeserializer(),
            personGroupSerde.deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldAggregateFirstNamesByLastNameStreamOne() {
        inputTopicOne.pipeInput(new TestRecord<>(
            "1", 
            buildKafkaPerson("Homer"), 
            Instant.parse("2000-01-01T01:00:00Z")
        ));
        inputTopicOne.pipeInput(new TestRecord<>(
            "2", 
            buildKafkaPerson("Marge"), 
            Instant.parse("2000-01-01T01:00:00Z")
        ));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertEquals("Simpson", results.get(0).key);
        assertIterableEquals(List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson", results.get(1).key);
        assertIterableEquals(List.of("Homer", "Marge"), results.get(1).value.getFirstNameByLastName().get("Simpson"));

        KeyValueStore<String, KafkaPersonGroup> stateStore = testDriver
            .getKeyValueStore(PERSON_COGROUP_AGGREGATE_STORE);

        assertIterableEquals(
            List.of("Homer", "Marge"), 
            stateStore.get("Simpson").getFirstNameByLastName().get("Simpson")
        );
    }

    @Test
    void shouldAggregateFirstNamesByLastNameStreamTwo() {
        inputTopicTwo.pipeInput(new TestRecord<>(
            "1", 
            buildKafkaPerson("Homer"),
            Instant.parse("2000-01-01T01:00:00Z")
        ));
        inputTopicTwo.pipeInput(new TestRecord<>(
            "2", 
            buildKafkaPerson("Marge"),
            Instant.parse("2000-01-01T01:00:00Z")
        ));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertEquals("Simpson", results.get(0).key);
        assertIterableEquals(List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson", results.get(1).key);
        assertIterableEquals(List.of("Homer", "Marge"), results.get(1).value.getFirstNameByLastName().get("Simpson"));

        KeyValueStore<String, KafkaPersonGroup> stateStore = testDriver
            .getKeyValueStore(PERSON_COGROUP_AGGREGATE_STORE);

        assertIterableEquals(
            List.of("Homer", "Marge"), 
            stateStore.get("Simpson").getFirstNameByLastName().get("Simpson")
        );
    }

    @Test
    void shouldAggregateFirstNamesByLastNameBothCogroupedStreams() {
        inputTopicOne.pipeInput(new TestRecord<>(
            "1", 
            buildKafkaPerson("Homer"),
            Instant.parse("2000-01-01T01:00:00Z")
        ));
        inputTopicOne.pipeInput(new TestRecord<>(
            "2", 
            buildKafkaPerson("Marge"),
            Instant.parse("2000-01-01T01:00:00Z")
        ));
        inputTopicTwo.pipeInput(new TestRecord<>(
            "3", 
            buildKafkaPerson("Bart"),
            Instant.parse("2000-01-01T01:00:00Z")
        ));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertEquals("Simpson", results.get(0).key);
        assertIterableEquals(List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson", results.get(1).key);
        assertIterableEquals(List.of("Homer", "Marge"), results.get(1).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson", results.get(2).key);
        assertIterableEquals(
            List.of("Homer", "Marge", "Bart"),
            results.get(2).value.getFirstNameByLastName().get("Simpson")
        );

        KeyValueStore<String, KafkaPersonGroup> stateStore = testDriver
            .getKeyValueStore(PERSON_COGROUP_AGGREGATE_STORE);

        assertIterableEquals(
            List.of("Homer", "Marge", "Bart"),
            stateStore.get("Simpson").getFirstNameByLastName().get("Simpson")
        );
    }

    private KafkaPerson buildKafkaPerson(String firstName) {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName(firstName)
            .setLastName("Simpson")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build();
    }
}
