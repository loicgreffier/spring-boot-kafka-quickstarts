package io.github.loicgreffier.streams.schedule.store.cleanup;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.schedule.store.cleanup.app.KafkaStreamsScheduleStoreCleanupTopology;
import io.github.loicgreffier.streams.schedule.store.cleanup.constants.StateStore;
import io.github.loicgreffier.streams.schedule.store.cleanup.serdes.SerdesUtils;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;

import static io.github.loicgreffier.streams.schedule.store.cleanup.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsScheduleStoreCleanupApplicationTest {
    private static final String SCHEMA_REGISTRY_SCOPE = KafkaStreamsScheduleStoreCleanupApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-schedule-store-cleanup-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create Serde for input and output topics
        Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        SerdesUtils.setSerdesConfig(config);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsScheduleStoreCleanupTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

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
        KafkaPerson personOne = buildKafkaPersonValue("Aaran", "Abbott", CountryCode.FR);
        KafkaPerson personTwo = buildKafkaPersonValue("Brendan", "Abbott", CountryCode.ES);
        KafkaPerson personThree = buildKafkaPersonValue("Bret", "Holman", CountryCode.FR);
        KafkaPerson personFour = buildKafkaPersonValue("Daimhin", "Abbott", CountryCode.GB);
        KafkaPerson personFive = buildKafkaPersonValue("Jiao", "Patton", CountryCode.GB);

        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopic.pipeInput(new TestRecord<>("1", personOne, start));
        inputTopic.pipeInput(new TestRecord<>("2", personTwo, start.plus(20, ChronoUnit.SECONDS)));
        inputTopic.pipeInput(new TestRecord<>("3", personThree, start.plus(40, ChronoUnit.SECONDS)));

        KeyValueStore<String, KafkaPerson> stateStore = testDriver.getKeyValueStore(StateStore.PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE.toString());

        // 1st STREAM_TIME punctuate when personOne is pushed
        assertThat(stateStore.get("1")).isNull();
        assertThat(stateStore.get("2")).isEqualTo(personTwo);
        assertThat(stateStore.get("3")).isEqualTo(personThree);

        inputTopic.pipeInput(new TestRecord<>("4", personFour, start.plus(2, ChronoUnit.MINUTES)));
        inputTopic.pipeInput(new TestRecord<>("5", personFive, start.plus(2, ChronoUnit.MINUTES).plusSeconds(30)));

        // 2nd STREAM_TIME punctuate when personFour is pushed
        assertThat(stateStore.get("2")).isNull();
        assertThat(stateStore.get("3")).isNull();
        assertThat(stateStore.get("4")).isNull();
        assertThat(stateStore.get("5")).isEqualTo(personFive);
    }

    private KafkaPerson buildKafkaPersonValue(String firstName, String lastName, CountryCode nationality) {
        return KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName(firstName)
                .setLastName(lastName)
                .setNationality(nationality)
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build();
    }
}
