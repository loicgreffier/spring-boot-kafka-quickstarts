package io.github.loicgreffier.streams.outer.join.stream.stream;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.outer.join.stream.stream.constant.StateStore.PERSON_OUTER_JOIN_STREAM_STREAM_STATE_STORE;
import static io.github.loicgreffier.streams.outer.join.stream.stream.constant.Topic.PERSON_OUTER_JOIN_STREAM_STREAM_REKEY_TOPIC;
import static io.github.loicgreffier.streams.outer.join.stream.stream.constant.Topic.PERSON_OUTER_JOIN_STREAM_STREAM_TOPIC;
import static io.github.loicgreffier.streams.outer.join.stream.stream.constant.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.outer.join.stream.stream.constant.Topic.PERSON_TOPIC_TWO;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaJoinPersons;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.outer.join.stream.stream.app.KafkaStreamsTopology;
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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class contains unit tests for the {@link KafkaStreamsTopology} class.
 */
class KafkaStreamsOuterJoinStreamStreamApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsOuterJoinStreamStreamApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> leftInputTopic;
    private TestInputTopic<String, KafkaPerson> rightInputTopic;
    private TestOutputTopic<String, KafkaPerson> rekeyLeftOutputTopic;
    private TestOutputTopic<String, KafkaPerson> rekeyRightOutputTopic;
    private TestOutputTopic<String, KafkaJoinPersons> joinOutputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-outer-join-stream-stream-test");
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
        Serde<KafkaJoinPersons> joinPersonsSerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        joinPersonsSerde.configure(config, false);

        leftInputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        rightInputTopic = testDriver.createInputTopic(
            PERSON_TOPIC_TWO,
            new StringSerializer(),
            personSerde.serializer()
        );
        rekeyLeftOutputTopic = testDriver.createOutputTopic(
            "streams-outer-join-stream-stream-test-" + PERSON_OUTER_JOIN_STREAM_STREAM_REKEY_TOPIC
                + "-left-repartition",
            new StringDeserializer(),
            personSerde.deserializer()
        );
        rekeyRightOutputTopic = testDriver.createOutputTopic(
            "streams-outer-join-stream-stream-test-" + PERSON_OUTER_JOIN_STREAM_STREAM_REKEY_TOPIC
                + "-right-repartition",
            new StringDeserializer(),
            personSerde.deserializer()
        );
        joinOutputTopic = testDriver.createOutputTopic(
            PERSON_OUTER_JOIN_STREAM_STREAM_TOPIC,
            new StringDeserializer(),
            joinPersonsSerde.deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldRekey() {
        KafkaPerson leftPerson = buildKafkaPerson("John", "Doe");
        KafkaPerson rightPerson = buildKafkaPerson("Michael", "Doe");

        leftInputTopic.pipeInput("1", leftPerson);
        rightInputTopic.pipeInput("2", rightPerson);

        List<KeyValue<String, KafkaPerson>> topicOneResults = rekeyLeftOutputTopic.readKeyValuesToList();
        List<KeyValue<String, KafkaPerson>> topicTwoResults = rekeyRightOutputTopic.readKeyValuesToList();

        assertEquals(KeyValue.pair("Doe", leftPerson), topicOneResults.get(0));
        assertEquals(KeyValue.pair("Doe", rightPerson), topicTwoResults.get(0));
    }

    @Test
    void shouldJoinWhenTimeWindowIsRespected() {
        KafkaPerson leftPersonDoe = buildKafkaPerson("John", "Doe");
        leftInputTopic.pipeInput(new TestRecord<>("1", leftPersonDoe, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaPerson leftPersonSmith = buildKafkaPerson("Jane", "Smith");
        leftInputTopic.pipeInput(new TestRecord<>("2", leftPersonSmith, Instant.parse("2000-01-01T01:01:00Z")));

        KafkaPerson rightPersonDoe = buildKafkaPerson("Michael", "Doe");
        rightInputTopic.pipeInput(new TestRecord<>("3", rightPersonDoe, Instant.parse("2000-01-01T01:01:30Z")));

        KafkaPerson rightPersonSmith = buildKafkaPerson("Daniel", "Smith");
        rightInputTopic.pipeInput(new TestRecord<>("4", rightPersonSmith, Instant.parse("2000-01-01T01:02:00Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("Doe", results.get(0).key);
        assertEquals(leftPersonDoe, results.get(0).value.getPersonOne());
        assertEquals(rightPersonDoe, results.get(0).value.getPersonTwo());

        assertEquals("Smith", results.get(1).key);
        assertEquals(leftPersonSmith, results.get(1).value.getPersonOne());
        assertEquals(rightPersonSmith, results.get(1).value.getPersonTwo());

        // Check state store
        WindowStore<String, KafkaPerson> leftStateStore = testDriver
            .getWindowStore(PERSON_OUTER_JOIN_STREAM_STREAM_STATE_STORE + "-outer-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> leftKeyValueDoe00To10 = iterator.next();
            assertEquals("Doe", leftKeyValueDoe00To10.key.key());
            assertEquals("2000-01-01T01:00:00Z", leftKeyValueDoe00To10.key.window().startTime().toString());
            assertEquals("2000-01-01T01:10:00Z", leftKeyValueDoe00To10.key.window().endTime().toString());
            assertEquals(leftPersonDoe, leftKeyValueDoe00To10.value);

            KeyValue<Windowed<String>, KafkaPerson> leftKeyValueSmith01To11 = iterator.next();
            assertEquals("Smith", leftKeyValueSmith01To11.key.key());
            assertEquals("2000-01-01T01:01:00Z", leftKeyValueSmith01To11.key.window().startTime().toString());
            assertEquals("2000-01-01T01:11:00Z", leftKeyValueSmith01To11.key.window().endTime().toString());
            assertEquals(leftPersonSmith, leftKeyValueSmith01To11.value);

            assertFalse(iterator.hasNext());
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver
            .getWindowStore(PERSON_OUTER_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> rightKeyValueDoe01m30To11m30 = iterator.next();
            assertEquals("Doe", rightKeyValueDoe01m30To11m30.key.key());
            assertEquals("2000-01-01T01:01:30Z", rightKeyValueDoe01m30To11m30.key.window().startTime().toString());
            assertEquals("2000-01-01T01:11:30Z", rightKeyValueDoe01m30To11m30.key.window().endTime().toString());
            assertEquals(rightPersonDoe, rightKeyValueDoe01m30To11m30.value);

            KeyValue<Windowed<String>, KafkaPerson> rightKeyValueSmith02To12 = iterator.next();
            assertEquals("Smith", rightKeyValueSmith02To12.key.key());
            assertEquals("2000-01-01T01:02:00Z", rightKeyValueSmith02To12.key.window().startTime().toString());
            assertEquals("2000-01-01T01:12:00Z", rightKeyValueSmith02To12.key.window().endTime().toString());
            assertEquals(rightPersonSmith, rightKeyValueSmith02To12.value);

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldEmitLeftPersonWhenTimeWindowIsNotRespected() {
        KafkaPerson personLeft = buildKafkaPerson("John", "Doe");
        leftInputTopic.pipeInput(new TestRecord<>("1", personLeft, Instant.parse("2000-01-01T01:00:00Z")));

        // Upper bound of sliding window is inclusive, so the timestamp of the second record
        // needs to be set to 01:06:01 (+1 second after the window end + grace period of the
        // first record) to not join with the first record. The left person with a null right person
        // is emitted at the end of the window (+ grace period).
        KafkaPerson personRight = buildKafkaPerson("Michael", "Doe");
        rightInputTopic.pipeInput(new TestRecord<>("2", personRight, Instant.parse("2000-01-01T01:06:01Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("Doe", results.get(0).key);
        assertEquals(personLeft, results.get(0).value.getPersonOne());
        assertNull(results.get(0).value.getPersonTwo());

        // Check state store
        WindowStore<String, KafkaPerson> leftStateStore = testDriver
            .getWindowStore(PERSON_OUTER_JOIN_STREAM_STREAM_STATE_STORE + "-outer-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> leftKeyValue = iterator.next();
            assertEquals("Doe", leftKeyValue.key.key());
            assertEquals("2000-01-01T01:00:00Z", leftKeyValue.key.window().startTime().toString());
            assertEquals("2000-01-01T01:10:00Z", leftKeyValue.key.window().endTime().toString());
            assertEquals(personLeft, leftKeyValue.value);

            assertFalse(iterator.hasNext());
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver
            .getWindowStore(PERSON_OUTER_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> rightKeyValue = iterator.next();
            assertEquals("Doe", rightKeyValue.key.key());
            assertEquals("2000-01-01T01:06:01Z", rightKeyValue.key.window().startTime().toString());
            assertEquals("2000-01-01T01:16:01Z", rightKeyValue.key.window().endTime().toString());
            assertEquals(personRight, rightKeyValue.value);

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldEmitRightPersonWhenTimeWindowIsNotRespected() {
        KafkaPerson personRight = buildKafkaPerson("Michael", "Doe");
        rightInputTopic.pipeInput(new TestRecord<>("1", personRight, Instant.parse("2000-01-01T01:00:00Z")));

        // Upper bound of sliding window is inclusive, so the timestamp of the second record
        // needs to be set to 01:06:01 (+1 second after the window end + grace period of the
        // first record) to not join with the first record. The right person with a null left person
        // is emitted at the end of the window (+ grace period).
        KafkaPerson personLeft = buildKafkaPerson("John", "Doe");
        leftInputTopic.pipeInput(new TestRecord<>("2", personLeft, Instant.parse("2000-01-01T01:06:01Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("Doe", results.get(0).key);
        assertNull(results.get(0).value.getPersonOne());
        assertEquals(personRight, results.get(0).value.getPersonTwo());

        // Check state store
        WindowStore<String, KafkaPerson> leftStateStore = testDriver
            .getWindowStore(PERSON_OUTER_JOIN_STREAM_STREAM_STATE_STORE + "-outer-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> leftKeyValue = iterator.next();
            assertEquals("Doe", leftKeyValue.key.key());
            assertEquals("2000-01-01T01:06:01Z", leftKeyValue.key.window().startTime().toString());
            assertEquals("2000-01-01T01:16:01Z", leftKeyValue.key.window().endTime().toString());
            assertEquals(personLeft, leftKeyValue.value);
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver
            .getWindowStore(PERSON_OUTER_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> rightKeyValue = iterator.next();
            assertEquals("Doe", rightKeyValue.key.key());
            assertEquals("2000-01-01T01:00:00Z", rightKeyValue.key.window().startTime().toString());
            assertEquals("2000-01-01T01:10:00Z", rightKeyValue.key.window().endTime().toString());
            assertEquals(personRight, rightKeyValue.value);
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        KafkaPerson personLeft1 = buildKafkaPerson("John", "Doe");
        leftInputTopic.pipeInput(new TestRecord<>("1", personLeft1, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaPerson personLeft2 = buildKafkaPerson("Jane", "Smith");
        leftInputTopic.pipeInput(new TestRecord<>("3", personLeft2, Instant.parse("2000-01-01T01:10:30Z")));

        // At this point, the stream time is 01:10:30, so the first record expired in the
        // left state store (its window is 01:00:00->01:10:00).
        // However, the following delayed record will be joined with the first record
        // because of the grace period of 1 minute.

        KafkaPerson personRight1 = buildKafkaPerson("Daniel", "Doe");
        rightInputTopic.pipeInput(new TestRecord<>("2", personRight1, Instant.parse("2000-01-01T01:05:00Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        // No record match before the end of the window, so the first record is emitted with a
        // null right person at the end of the window (+ grace period).
        assertEquals("Doe", results.get(0).key);
        assertEquals(personLeft1, results.get(0).value.getPersonOne());
        assertNull(results.get(0).value.getPersonTwo());

        // The delayed record finally comes and is joined with the first record,
        // so an updated record is emitted with the right person.
        assertEquals("Doe", results.get(1).key);
        assertEquals(personLeft1, results.get(1).value.getPersonOne());
        assertEquals(personRight1, results.get(1).value.getPersonTwo());

        // Check state store
        WindowStore<String, KafkaPerson> leftStateStore = testDriver
            .getWindowStore(PERSON_OUTER_JOIN_STREAM_STREAM_STATE_STORE + "-outer-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> windowDoe01h00To01h10 = iterator.next();
            assertEquals("Doe", windowDoe01h00To01h10.key.key());
            assertEquals("2000-01-01T01:00:00Z", windowDoe01h00To01h10.key.window().startTime().toString());
            assertEquals("2000-01-01T01:10:00Z", windowDoe01h00To01h10.key.window().endTime().toString());
            assertEquals(personLeft1, windowDoe01h00To01h10.value);

            KeyValue<Windowed<String>, KafkaPerson> windowDoe01h10m30To01h20m30 = iterator.next();
            assertEquals("Smith", windowDoe01h10m30To01h20m30.key.key());
            assertEquals("2000-01-01T01:10:30Z", windowDoe01h10m30To01h20m30.key.window().startTime().toString());
            assertEquals("2000-01-01T01:20:30Z", windowDoe01h10m30To01h20m30.key.window().endTime().toString());
            assertEquals(personLeft2, windowDoe01h10m30To01h20m30.value);

            assertFalse(iterator.hasNext());
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver
            .getWindowStore(PERSON_OUTER_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> rightKeyValue = iterator.next();
            assertEquals("Doe", rightKeyValue.key.key());
            assertEquals("2000-01-01T01:05:00Z", rightKeyValue.key.window().startTime().toString());
            assertEquals("2000-01-01T01:15:00Z", rightKeyValue.key.window().endTime().toString());
            assertEquals(personRight1, rightKeyValue.value);

            assertFalse(iterator.hasNext());
        }
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
