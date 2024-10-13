package io.github.loicgreffier.streams.leftjoin.stream.stream;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.leftjoin.stream.stream.constant.StateStore.PERSON_LEFT_JOIN_STREAM_STREAM_STORE;
import static io.github.loicgreffier.streams.leftjoin.stream.stream.constant.Topic.PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC;
import static io.github.loicgreffier.streams.leftjoin.stream.stream.constant.Topic.PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC;
import static io.github.loicgreffier.streams.leftjoin.stream.stream.constant.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.leftjoin.stream.stream.constant.Topic.PERSON_TOPIC_TWO;
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
import io.github.loicgreffier.streams.leftjoin.stream.stream.app.KafkaStreamsTopology;
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

class KafkaStreamsLeftJoinStreamStreamApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsLeftJoinStreamStreamApplicationTest.class.getName();
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
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-left-join-stream-stream-test");
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
            "streams-left-join-stream-stream-test-" + PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC + "-left-repartition",
            new StringDeserializer(),
            personSerde.deserializer()
        );
        rekeyRightOutputTopic = testDriver.createOutputTopic(
            "streams-left-join-stream-stream-test-" + PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC + "-right-repartition",
            new StringDeserializer(),
            personSerde.deserializer()
        );
        joinOutputTopic = testDriver.createOutputTopic(
            PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC,
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
        KafkaPerson leftPerson = buildKafkaPerson("Homer");
        KafkaPerson rightPerson = buildKafkaPerson("Marge");

        leftInputTopic.pipeInput("1", leftPerson);
        rightInputTopic.pipeInput("2", rightPerson);

        List<KeyValue<String, KafkaPerson>> topicOneResults = rekeyLeftOutputTopic.readKeyValuesToList();
        List<KeyValue<String, KafkaPerson>> topicTwoResults = rekeyRightOutputTopic.readKeyValuesToList();

        assertEquals(KeyValue.pair("Simpson", leftPerson), topicOneResults.get(0));
        assertEquals(KeyValue.pair("Simpson", rightPerson), topicTwoResults.get(0));
    }

    @Test
    void shouldJoinWhenTimeWindowIsRespected() {
        KafkaPerson homer = buildKafkaPerson("Homer");
        leftInputTopic.pipeInput(new TestRecord<>("1", homer, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaPerson marge = buildKafkaPerson("Marge");
        rightInputTopic.pipeInput(new TestRecord<>("2", marge, Instant.parse("2000-01-01T01:02:00Z")));

        KafkaPerson bart = buildKafkaPerson("Bart");
        leftInputTopic.pipeInput(new TestRecord<>("3", bart, Instant.parse("2000-01-01T01:03:00Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("Simpson", results.get(0).key);
        assertEquals(homer, results.get(0).value.getPersonOne());
        assertEquals(marge, results.get(0).value.getPersonTwo());

        assertEquals("Simpson", results.get(1).key);
        assertEquals(bart, results.get(1).value.getPersonOne());
        assertEquals(marge, results.get(1).value.getPersonTwo());

        WindowStore<String, KafkaPerson> leftStateStore = testDriver
            .getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            // As join windows are looking backward and forward in time,
            // records are kept in the store for "before" + "after" duration.

            KeyValue<Windowed<String>, KafkaPerson> leftKeyValue00To10 = iterator.next();
            assertEquals("Simpson", leftKeyValue00To10.key.key());
            assertEquals("2000-01-01T01:00:00Z", leftKeyValue00To10.key.window().startTime().toString());
            assertEquals("2000-01-01T01:10:00Z", leftKeyValue00To10.key.window().endTime().toString());
            assertEquals(homer, leftKeyValue00To10.value);

            KeyValue<Windowed<String>, KafkaPerson> leftKeyValue03To13 = iterator.next();
            assertEquals("Simpson", leftKeyValue03To13.key.key());
            assertEquals("2000-01-01T01:03:00Z", leftKeyValue03To13.key.window().startTime().toString());
            assertEquals("2000-01-01T01:13:00Z", leftKeyValue03To13.key.window().endTime().toString());
            assertEquals(bart, leftKeyValue03To13.value);

            assertFalse(iterator.hasNext());
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver
            .getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STORE + "-outer-other-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> rightKeyValue02To12 = iterator.next();
            assertEquals("Simpson", rightKeyValue02To12.key.key());
            assertEquals("2000-01-01T01:02:00Z", rightKeyValue02To12.key.window().startTime().toString());
            assertEquals("2000-01-01T01:12:00Z", rightKeyValue02To12.key.window().endTime().toString());
            assertEquals(marge, rightKeyValue02To12.value);

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldEmitLeftPersonWhenTimeWindowIsNotRespected() {
        KafkaPerson homer = buildKafkaPerson("Homer");
        leftInputTopic.pipeInput(new TestRecord<>("1", homer, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaPerson marge = buildKafkaPerson("Marge");
        rightInputTopic.pipeInput(new TestRecord<>("2", marge, Instant.parse("2000-01-01T01:06:00Z")));

        KafkaPerson bart = buildKafkaPerson("Bart");
        leftInputTopic.pipeInput(new TestRecord<>("3", bart, Instant.parse("2000-01-01T01:13:00Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        // The right value is null because Marge arrived too late for Homer.
        assertEquals("Simpson", results.get(0).key);
        assertEquals(homer, results.get(0).value.getPersonOne());
        assertNull(results.get(0).value.getPersonTwo());

        // Output for Marge has not been emitted because of the left join nature.
        // Output for Bart has not been emitted yet because the join window is still open.

        WindowStore<String, KafkaPerson> leftStateStore = testDriver
            .getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> leftKeyValue00To10 = iterator.next();
            assertEquals("Simpson", leftKeyValue00To10.key.key());
            assertEquals("2000-01-01T01:13:00Z", leftKeyValue00To10.key.window().startTime().toString());
            assertEquals("2000-01-01T01:23:00Z", leftKeyValue00To10.key.window().endTime().toString());
            assertEquals(bart, leftKeyValue00To10.value);

            assertFalse(iterator.hasNext());
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver
            .getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STORE + "-outer-other-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> rightKeyValue = iterator.next();
            assertEquals("Simpson", rightKeyValue.key.key());
            assertEquals("2000-01-01T01:06:00Z", rightKeyValue.key.window().startTime().toString());
            assertEquals("2000-01-01T01:16:00Z", rightKeyValue.key.window().endTime().toString());
            assertEquals(marge, rightKeyValue.value);

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        KafkaPerson homer = buildKafkaPerson("Homer");
        leftInputTopic.pipeInput(new TestRecord<>("1", homer, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaPerson marge = buildKafkaPerson("Marge");
        leftInputTopic.pipeInput(new TestRecord<>("3", marge, Instant.parse("2000-01-01T01:10:30Z")));

        // At this point, the stream time is 01:10:30. It exceeds by 30 seconds
        // the upper bound of the Homer's window [01:00:00.001Z->01:10:00Z] in the store.
        // However, the following delayed record "Bart" will be joined with the first record
        // thanks to the grace period of 1 minute.

        KafkaPerson bart = buildKafkaPerson("Bart");
        rightInputTopic.pipeInput(new TestRecord<>("2", bart, Instant.parse("2000-01-01T01:05:00Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        // No record in the secondary stream matched the first record in the primary stream
        // at the end of the join window + grace period (01:06:00). Null value is emitted.
        assertEquals("Simpson", results.get(0).key);
        assertEquals(homer, results.get(0).value.getPersonOne());
        assertNull(results.get(0).value.getPersonTwo());

        // The delayed record finally comes and joined with the first record,
        // so an updated record is emitted with the right value.
        assertEquals("Simpson", results.get(1).key);
        assertEquals(homer, results.get(1).value.getPersonOne());
        assertEquals(bart, results.get(1).value.getPersonTwo());

        WindowStore<String, KafkaPerson> leftStateStore = testDriver
            .getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STORE + "-this-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> leftKeyValue00To10 = iterator.next();
            assertEquals("Simpson", leftKeyValue00To10.key.key());
            assertEquals("2000-01-01T01:00:00Z", leftKeyValue00To10.key.window().startTime().toString());
            assertEquals("2000-01-01T01:10:00Z", leftKeyValue00To10.key.window().endTime().toString());
            assertEquals(homer, leftKeyValue00To10.value);

            KeyValue<Windowed<String>, KafkaPerson> leftKeyValue10m30To20m30 = iterator.next();
            assertEquals("Simpson", leftKeyValue10m30To20m30.key.key());
            assertEquals("2000-01-01T01:10:30Z", leftKeyValue10m30To20m30.key.window().startTime().toString());
            assertEquals("2000-01-01T01:20:30Z", leftKeyValue10m30To20m30.key.window().endTime().toString());
            assertEquals(marge, leftKeyValue10m30To20m30.value);

            assertFalse(iterator.hasNext());
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver
            .getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STORE + "-outer-other-join-store");

        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> rightKeyValue = iterator.next();
            assertEquals("Simpson", rightKeyValue.key.key());
            assertEquals("2000-01-01T01:05:00Z", rightKeyValue.key.window().startTime().toString());
            assertEquals("2000-01-01T01:15:00Z", rightKeyValue.key.window().endTime().toString());
            assertEquals(bart, rightKeyValue.value);

            assertFalse(iterator.hasNext());
        }
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
