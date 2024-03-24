package io.github.loicgreffier.streams.schedule.store.cleanup;

import static io.github.loicgreffier.streams.schedule.store.cleanup.constant.StateStore.PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE;
import static io.github.loicgreffier.streams.schedule.store.cleanup.constant.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.schedule.store.cleanup.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.schedule.store.cleanup.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class contains unit tests for the {@link KafkaStreamsTopology} class.
 */
class KafkaStreamsScheduleStoreCleanupApplicationTests {
    private static final String SCHEMA_REGISTRY_SCOPE =
        KafkaStreamsScheduleStoreCleanupApplicationTests.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,
            "streams-schedule-store-cleanup-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            SpecificAvroSerde.class.getName());
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            MOCK_SCHEMA_REGISTRY_URL);

        // Create Serde for input and output topics
        Map<String, String> config =
            Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                MOCK_SCHEMA_REGISTRY_URL);
        SerdesUtils.setSerdesConfig(config);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties,
            Instant.parse("2000-01-01T01:00:00.00Z"));

        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(),
            SerdesUtils.<KafkaPerson>specificAvroValueSerdes().serializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldFillAndCleanupStore() {
        KafkaPerson firstPerson = buildKafkaPerson("John", "Doe");
        inputTopic.pipeInput(
            new TestRecord<>("1", firstPerson, Instant.parse("2000-01-01T01:00:00.00Z")));

        KafkaPerson secondPerson = buildKafkaPerson("Michael", "Doe");
        inputTopic.pipeInput(
            new TestRecord<>("2", secondPerson, Instant.parse("2000-01-01T01:00:20.00Z")));

        KafkaPerson thirdPerson = buildKafkaPerson("Jane", "Smith");
        inputTopic.pipeInput(
            new TestRecord<>("3", thirdPerson, Instant.parse("2000-01-01T01:00:40.00Z")));

        KeyValueStore<String, KafkaPerson> stateStore =
            testDriver.getKeyValueStore(PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE);

        // 1st stream time punctuate triggered after first record is pushed.
        // That is why the first record is not in the store anymore.
        assertThat(stateStore.get("1")).isNull();
        assertThat(stateStore.get("2")).isEqualTo(secondPerson);
        assertThat(stateStore.get("3")).isEqualTo(thirdPerson);

        KafkaPerson fourthPerson = buildKafkaPerson("Emily", "Doe");
        inputTopic.pipeInput(
            new TestRecord<>("4", fourthPerson, Instant.parse("2000-01-01T01:02:00.00Z")));

        KafkaPerson fifthPerson = buildKafkaPerson("Daniel", "Doe");
        inputTopic.pipeInput(
            new TestRecord<>("5", fifthPerson, Instant.parse("2000-01-01T01:02:30.00Z")));

        // 2nd stream time punctuate
        assertThat(stateStore.get("2")).isNull();
        assertThat(stateStore.get("3")).isNull();
        assertThat(stateStore.get("4")).isNull();
        assertThat(stateStore.get("5")).isEqualTo(fifthPerson);
    }

    private KafkaPerson buildKafkaPerson(String firstName, String lastName) {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName(firstName)
            .setLastName(lastName)
            .setNationality(CountryCode.GB)
            .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
            .build();
    }
}
