package io.github.loicgreffier.streams.average;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaAverageAge;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.average.app.KafkaStreamsTopology;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.github.loicgreffier.streams.average.constants.StateStore.PERSON_AVERAGE_STATE_STORE;
import static io.github.loicgreffier.streams.average.constants.Topic.PERSON_AVERAGE_TOPIC;
import static io.github.loicgreffier.streams.average.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsAverageApplicationTests {
    private static final String SCHEMA_REGISTRY_SCOPE = KafkaStreamsAverageApplicationTests.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-average-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        // Create Serde for input and output topics
        Serde<KafkaPerson> personSerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);

        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(PERSON_AVERAGE_TOPIC, new StringDeserializer(), new LongDeserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldComputeAverageAgeByNationality() {
        LocalDate currentDate = LocalDate.now();

        KafkaPerson yearsOld25 = buildKafkaPerson("Aaran", currentDate.minusYears(25).atStartOfDay().toInstant(ZoneOffset.UTC));
        KafkaPerson yearsOld75 = buildKafkaPerson("Brendan", currentDate.minusYears(75).atStartOfDay().toInstant(ZoneOffset.UTC));

        inputTopic.pipeInput("1", yearsOld25);
        inputTopic.pipeInput("2", yearsOld75);

        List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

        assertThat(results.get(0)).isEqualTo(KeyValue.pair("FR", 25L));
        assertThat(results.get(1)).isEqualTo(KeyValue.pair("FR", 50L));

        // Explore state store
        KeyValueStore<String, KafkaAverageAge> stateStore = testDriver.getKeyValueStore(PERSON_AVERAGE_STATE_STORE);

        assertThat(stateStore.get("FR").getCount()).isEqualTo(2L);
        assertThat(stateStore.get("FR").getAgeSum()).isEqualTo(100L);
    }

    private KafkaPerson buildKafkaPerson(String firstName, Instant birthDate) {
        return KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName(firstName)
                .setLastName("Allen")
                .setNationality(CountryCode.FR)
                .setBirthDate(birthDate)
                .build();
    }
}
