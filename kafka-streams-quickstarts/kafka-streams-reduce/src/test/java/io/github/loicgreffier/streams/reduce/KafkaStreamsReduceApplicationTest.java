package io.github.loicgreffier.streams.reduce;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.reduce.constant.StateStore.PERSON_REDUCE_STATE_STORE;
import static io.github.loicgreffier.streams.reduce.constant.Topic.PERSON_REDUCE_TOPIC;
import static io.github.loicgreffier.streams.reduce.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.reduce.app.KafkaStreamsTopology;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class contains unit tests for the {@link KafkaStreamsTopology} class.
 */
class KafkaStreamsReduceApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsReduceApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPerson> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-reduce-test");
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
        Map<String, String> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);

        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(
            PERSON_REDUCE_TOPIC,
            new StringDeserializer(),
            personSerde.deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldReduceByNationalityAndKeepOldest() {
        KafkaPerson oldestFr = buildKafkaPerson(
            "John",
            "Doe",
            Instant.parse("1956-08-29T18:35:24Z"),
            CountryCode.FR);

        KafkaPerson youngestFr = buildKafkaPerson(
            "Michael",
            "Doe",
            Instant.parse("1994-11-09T08:08:50Z"),
            CountryCode.FR);

        KafkaPerson youngestGb = buildKafkaPerson(
            "Jane",
            "Smith",
            Instant.parse("1996-02-02T04:58:01Z"),
            CountryCode.GB);

        KafkaPerson oldestGb = buildKafkaPerson(
            "Daniel",
            "Smith",
            Instant.parse("1986-05-26T04:52:06Z"),
            CountryCode.GB);

        inputTopic.pipeInput("1", oldestFr);
        inputTopic.pipeInput("2", youngestFr);
        inputTopic.pipeInput("3", youngestGb);
        inputTopic.pipeInput("4", oldestGb);

        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();

        assertEquals(KeyValue.pair(CountryCode.FR.toString(), oldestFr), results.get(0));
        assertEquals(KeyValue.pair(CountryCode.FR.toString(), oldestFr), results.get(1));
        assertEquals(KeyValue.pair(CountryCode.GB.toString(), youngestGb), results.get(2));
        assertEquals(KeyValue.pair(CountryCode.GB.toString(), oldestGb), results.get(3));

        // Check state store
        KeyValueStore<String, KafkaPerson> stateStore = testDriver.getKeyValueStore(PERSON_REDUCE_STATE_STORE);

        assertEquals(oldestFr, stateStore.get(CountryCode.FR.toString()));
        assertEquals(oldestGb, stateStore.get(CountryCode.GB.toString()));
    }

    private KafkaPerson buildKafkaPerson(String firstName,
                                         String lastName,
                                         Instant birthDate,
                                         CountryCode nationality) {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName(firstName)
            .setLastName(lastName)
            .setBirthDate(birthDate)
            .setNationality(nationality)
            .build();
    }
}
