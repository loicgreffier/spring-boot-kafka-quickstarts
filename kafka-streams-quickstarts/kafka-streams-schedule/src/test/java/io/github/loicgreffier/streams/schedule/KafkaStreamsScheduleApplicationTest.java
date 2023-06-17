package io.github.loicgreffier.streams.schedule;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.schedule.app.KafkaStreamsScheduleTopology;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.github.loicgreffier.streams.schedule.constants.StateStore.PERSON_SCHEDULE_STATE_STORE;
import static io.github.loicgreffier.streams.schedule.constants.Topic.PERSON_SCHEDULE_TOPIC;
import static io.github.loicgreffier.streams.schedule.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsScheduleApplicationTest {
    private static final String SCHEMA_REGISTRY_SCOPE = KafkaStreamsScheduleApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-schedule-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsScheduleTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        // Create Serde for input and output topics
        Serde<KafkaPerson> personSerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);

        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(PERSON_SCHEDULE_TOPIC, new StringDeserializer(), new LongDeserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldCountPersonByNationality() {
        KafkaPerson personOne = buildKafkaPersonValue("Aaran", "Abbott", CountryCode.FR);
        KafkaPerson personTwo = buildKafkaPersonValue("Brendan", "Abbott", CountryCode.ES);
        KafkaPerson personThree = buildKafkaPersonValue("Bret", "Holman", CountryCode.FR);
        KafkaPerson personFour = buildKafkaPersonValue("Daimhin", "Abbott", CountryCode.GB);
        KafkaPerson personFive = buildKafkaPersonValue("Jiao", "Patton", CountryCode.GB);
        KafkaPerson personSix = buildKafkaPersonValue("Jude", "Holman", CountryCode.IT);
        KafkaPerson personSeven = buildKafkaPersonValue("Jeswin", "Holman", CountryCode.FR);

        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopic.pipeInput(new TestRecord<>("1", personOne, start));
        inputTopic.pipeInput(new TestRecord<>("2", personTwo, start.plus(1, ChronoUnit.MINUTES)));
        inputTopic.pipeInput(new TestRecord<>("3", personThree, start.plus(1, ChronoUnit.MINUTES).plusSeconds(30)));
        inputTopic.pipeInput(new TestRecord<>("4", personFour, start.plus(2, ChronoUnit.MINUTES)));
        testDriver.advanceWallClockTime(Duration.ofMinutes(2));
        inputTopic.pipeInput(new TestRecord<>("5", personFive, start.plus(3, ChronoUnit.MINUTES)));
        testDriver.advanceWallClockTime(Duration.ofMinutes(2));
        inputTopic.pipeInput(new TestRecord<>("6", personSix, start.plus(5, ChronoUnit.MINUTES)));
        inputTopic.pipeInput(new TestRecord<>("7", personSeven, start.plus(6, ChronoUnit.MINUTES)));
        testDriver.advanceWallClockTime(Duration.ofMinutes(2));

        List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(17);

        // 1st STREAM_TIME punctuate
        assertThat(results.get(0).key).isEqualTo("FR");
        assertThat(results.get(0).value).isEqualTo(1);

        // 2nd STREAM_TIME punctuate
        assertThat(results.get(1).key).isEqualTo("ES");
        assertThat(results.get(1).value).isEqualTo(1);

        assertThat(results.get(2).key).isEqualTo("FR");
        assertThat(results.get(2).value).isEqualTo(1);

        // 3rd STREAM_TIME punctuate
        assertThat(results.get(3).key).isEqualTo("ES");
        assertThat(results.get(3).value).isEqualTo(1);

        assertThat(results.get(4).key).isEqualTo("FR");
        assertThat(results.get(4).value).isEqualTo(2);

        assertThat(results.get(5).key).isEqualTo("GB");
        assertThat(results.get(5).value).isEqualTo(1);

        // 1st WALL_CLOCK punctuate now

        // 4th STREAM_TIME punctuate
        assertThat(results.get(6).key).isEqualTo("ES");
        assertThat(results.get(6).value).isZero();

        assertThat(results.get(7).key).isEqualTo("FR");
        assertThat(results.get(7).value).isZero();

        assertThat(results.get(8).key).isEqualTo("GB");
        assertThat(results.get(8).value).isEqualTo(1);

        // 2nd WALL_CLOCK punctuate now

        // 5th STREAM_TIME punctuate
        assertThat(results.get(9).key).isEqualTo("ES");
        assertThat(results.get(9).value).isZero();

        assertThat(results.get(10).key).isEqualTo("FR");
        assertThat(results.get(10).value).isZero();

        assertThat(results.get(11).key).isEqualTo("GB");
        assertThat(results.get(11).value).isZero();

        assertThat(results.get(12).key).isEqualTo("IT");
        assertThat(results.get(12).value).isEqualTo(1);

        // 6th STREAM_TIME punctuate
        assertThat(results.get(13).key).isEqualTo("ES");
        assertThat(results.get(13).value).isZero();

        assertThat(results.get(14).key).isEqualTo("FR");
        assertThat(results.get(14).value).isEqualTo(1);

        assertThat(results.get(15).key).isEqualTo("GB");
        assertThat(results.get(15).value).isZero();

        assertThat(results.get(16).key).isEqualTo("IT");
        assertThat(results.get(16).value).isEqualTo(1);

        // 3rd WALL_CLOCK punctuate now

        KeyValueStore<String, ValueAndTimestamp<Long>> stateStore = testDriver.getTimestampedKeyValueStore(PERSON_SCHEDULE_STATE_STORE);

        assertThat(stateStore.get("ES").value()).isZero();
        assertThat(stateStore.get("ES").timestamp()).isNotNegative();
        assertThat(stateStore.get("FR").value()).isZero();
        assertThat(stateStore.get("FR").timestamp()).isNotNegative();
        assertThat(stateStore.get("GB").value()).isZero();
        assertThat(stateStore.get("GB").timestamp()).isNotNegative();
        assertThat(stateStore.get("IT").value()).isZero();
        assertThat(stateStore.get("IT").timestamp()).isNotNegative();
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
