package io.github.loicgreffier.streams.reduce;

import static io.github.loicgreffier.streams.reduce.constants.StateStore.PERSON_REDUCE_STATE_STORE;
import static io.github.loicgreffier.streams.reduce.constants.Topic.PERSON_REDUCE_TOPIC;
import static io.github.loicgreffier.streams.reduce.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class contains integration tests for the {@link KafkaStreamsTopology} class.
 */
class KafkaStreamsReduceApplicationTests {
    private static final String SCHEMA_REGISTRY_SCOPE =
        KafkaStreamsReduceApplicationTests.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPerson> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-reduce-test");
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
        outputTopic = testDriver.createOutputTopic(PERSON_REDUCE_TOPIC, new StringDeserializer(),
            personSerde.deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldReduceByNationalityAndKeepOldest() {
        KafkaPerson oldestFr =
            buildKafkaPerson("John", "Doe", Instant.parse("1956-08-29T18:35:24.00Z"),
                CountryCode.FR);
        KafkaPerson youngestFr =
            buildKafkaPerson("Michael", "Doe", Instant.parse("1994-11-09T08:08:50.00Z"),
                CountryCode.FR);
        KafkaPerson youngestGb =
            buildKafkaPerson("Jane", "Smith", Instant.parse("1996-02-02T04:58:01.00Z"),
                CountryCode.GB);
        KafkaPerson oldestGb =
            buildKafkaPerson("Daniel", "Smith", Instant.parse("1986-05-26T04:52:06Z"),
                CountryCode.GB);

        inputTopic.pipeInput("1", oldestFr);
        inputTopic.pipeInput("2", youngestFr);
        inputTopic.pipeInput("3", youngestGb);
        inputTopic.pipeInput("4", oldestGb);

        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();

        assertThat(results.get(0)).isEqualTo(KeyValue.pair(CountryCode.FR.toString(), oldestFr));
        assertThat(results.get(1)).isEqualTo(KeyValue.pair(CountryCode.FR.toString(), oldestFr));
        assertThat(results.get(2)).isEqualTo(KeyValue.pair(CountryCode.GB.toString(), youngestGb));
        assertThat(results.get(3)).isEqualTo(KeyValue.pair(CountryCode.GB.toString(), oldestGb));

        // Check state store
        KeyValueStore<String, KafkaPerson> stateStore =
            testDriver.getKeyValueStore(PERSON_REDUCE_STATE_STORE);

        assertThat(stateStore.get(CountryCode.FR.toString())).isEqualTo(oldestFr);
        assertThat(stateStore.get(CountryCode.GB.toString())).isEqualTo(oldestGb);
    }

    private KafkaPerson buildKafkaPerson(String firstName, String lastName, Instant birthDate,
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
