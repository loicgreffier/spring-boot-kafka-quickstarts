package io.github.loicgreffier.streams.left.join.stream.stream;

import static io.github.loicgreffier.streams.left.join.stream.stream.constants.StateStore.PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE;
import static io.github.loicgreffier.streams.left.join.stream.stream.constants.Topic.PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.stream.constants.Topic.PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.stream.constants.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.stream.constants.Topic.PERSON_TOPIC_TWO;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaJoinPersons;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.left.join.stream.stream.app.KafkaStreamsTopology;
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
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class contains integration tests for the {@link KafkaStreamsTopology} class.
 */
class KafkaStreamsLeftJoinStreamStreamApplicationTests {
    private static final String SCHEMA_REGISTRY_SCOPE =
        KafkaStreamsLeftJoinStreamStreamApplicationTests.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
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
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,
            "streams-left-join-stream-stream-test");
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
        Serde<KafkaJoinPersons> joinPersonsSerde = new SpecificAvroSerde<>();
        Map<String, String> config =
            Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        joinPersonsSerde.configure(config, false);

        leftInputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(),
            personSerde.serializer());
        rightInputTopic = testDriver.createInputTopic(PERSON_TOPIC_TWO, new StringSerializer(),
            personSerde.serializer());
        rekeyLeftOutputTopic = testDriver.createOutputTopic(
            "streams-left-join-stream-stream-test-" + PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC
                + "-left-repartition", new StringDeserializer(),
            personSerde.deserializer());
        rekeyRightOutputTopic = testDriver.createOutputTopic(
            "streams-left-join-stream-stream-test-" + PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC
                + "-right-repartition", new StringDeserializer(),
            personSerde.deserializer());
        joinOutputTopic = testDriver.createOutputTopic(PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC,
            new StringDeserializer(),
            joinPersonsSerde.deserializer());
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

        List<KeyValue<String, KafkaPerson>> topicOneResults =
            rekeyLeftOutputTopic.readKeyValuesToList();
        List<KeyValue<String, KafkaPerson>> topicTwoResults =
            rekeyRightOutputTopic.readKeyValuesToList();

        assertThat(topicOneResults.get(0)).isEqualTo(KeyValue.pair("Doe", leftPerson));
        assertThat(topicTwoResults.get(0)).isEqualTo(KeyValue.pair("Doe", rightPerson));
    }

    @Test
    void shouldJoinWhenTimeWindowIsRespected() {
        KafkaPerson leftPersonDoe = buildKafkaPerson("John", "Doe");
        leftInputTopic.pipeInput(
            new TestRecord<>("1", leftPersonDoe, Instant.parse("2000-01-01T01:00:00.00Z")));

        KafkaPerson leftPersonSmith = buildKafkaPerson("Jane", "Smith");
        leftInputTopic.pipeInput(
            new TestRecord<>("2", leftPersonSmith, Instant.parse("2000-01-01T01:01:00.00Z")));

        KafkaPerson rightPersonDoe = buildKafkaPerson("Michael", "Doe");
        rightInputTopic.pipeInput(
            new TestRecord<>("3", rightPersonDoe, Instant.parse("2000-01-01T01:01:30.00Z")));

        KafkaPerson rightPersonSmith = buildKafkaPerson("Daniel", "Smith");
        rightInputTopic.pipeInput(
            new TestRecord<>("4", rightPersonSmith, Instant.parse("2000-01-01T01:02:00.00Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Doe");
        assertThat(results.get(0).value.getPersonOne()).isEqualTo(leftPersonDoe);
        assertThat(results.get(0).value.getPersonTwo()).isEqualTo(rightPersonDoe);

        assertThat(results.get(1).key).isEqualTo("Smith");
        assertThat(results.get(1).value.getPersonOne()).isEqualTo(leftPersonSmith);
        assertThat(results.get(1).value.getPersonTwo()).isEqualTo(rightPersonSmith);

        // Check state store
        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(
            PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> leftKeyValueDoe00To10 = iterator.next();
            assertThat(leftKeyValueDoe00To10.key.key()).isEqualTo("Doe");
            assertThat(leftKeyValueDoe00To10.key.window().startTime()).isEqualTo(
                "2000-01-01T01:00:00.00Z");
            assertThat(leftKeyValueDoe00To10.key.window().endTime()).isEqualTo(
                "2000-01-01T01:10:00.00Z");
            assertThat(leftKeyValueDoe00To10.value).isEqualTo(leftPersonDoe);

            KeyValue<Windowed<String>, KafkaPerson> leftKeyValueSmith01To11 = iterator.next();
            assertThat(leftKeyValueSmith01To11.key.key()).isEqualTo("Smith");
            assertThat(leftKeyValueSmith01To11.key.window().startTime()).isEqualTo(
                "2000-01-01T01:01:00.00Z");
            assertThat(leftKeyValueSmith01To11.key.window().endTime()).isEqualTo(
                "2000-01-01T01:11:00.00Z");
            assertThat(leftKeyValueSmith01To11.value).isEqualTo(leftPersonSmith);

            assertThat(iterator.hasNext()).isFalse();
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(
            PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> rightKeyValueDoe01m30To11m30 = iterator.next();
            assertThat(rightKeyValueDoe01m30To11m30.key.key()).isEqualTo("Doe");
            assertThat(rightKeyValueDoe01m30To11m30.key.window().startTime()).isEqualTo(
                "2000-01-01T01:01:30.00Z");
            assertThat(rightKeyValueDoe01m30To11m30.key.window().endTime()).isEqualTo(
                "2000-01-01T01:11:30.00Z");
            assertThat(rightKeyValueDoe01m30To11m30.value).isEqualTo(rightPersonDoe);

            KeyValue<Windowed<String>, KafkaPerson> rightKeyValueSmith02To12 = iterator.next();
            assertThat(rightKeyValueSmith02To12.key.key()).isEqualTo("Smith");
            assertThat(rightKeyValueSmith02To12.key.window().startTime()).isEqualTo(
                "2000-01-01T01:02:00.00Z");
            assertThat(rightKeyValueSmith02To12.key.window().endTime()).isEqualTo(
                "2000-01-01T01:12:00.00Z");
            assertThat(rightKeyValueSmith02To12.value).isEqualTo(rightPersonSmith);

            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void shouldEmitLeftPersonWhenTimeWindowIsNotRespected() {
        KafkaPerson leftPerson = buildKafkaPerson("John", "Doe");
        leftInputTopic.pipeInput(
            new TestRecord<>("1", leftPerson, Instant.parse("2000-01-01T01:00:00.00Z")));

        // Upper bound of sliding window is inclusive, so the timestamp of the second record
        // needs to be set to 01:06:01 (+1 second after the window end + grace period of the
        // first record) to not join with the first record. The left person with a null right person
        // is emitted at the end of the window (+ grace period).
        KafkaPerson rightPerson = buildKafkaPerson("Michael", "Doe");
        rightInputTopic.pipeInput(
            new TestRecord<>("2", rightPerson, Instant.parse("2000-01-01T01:06:01.00Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Doe");
        assertThat(results.get(0).value.getPersonOne()).isEqualTo(leftPerson);
        assertThat(results.get(0).value.getPersonTwo()).isNull();

        // Check state store
        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(
            PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> leftKeyValue = iterator.next();
            assertThat(leftKeyValue.key.key()).isEqualTo("Doe");
            assertThat(leftKeyValue.key.window().startTime()).isEqualTo(
                "2000-01-01T01:00:00.00Z");
            assertThat(leftKeyValue.key.window().endTime()).isEqualTo(
                "2000-01-01T01:10:00.00Z");
            assertThat(leftKeyValue.value).isEqualTo(leftPerson);

            assertThat(iterator.hasNext()).isFalse();
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(
            PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> rightKeyValue = iterator.next();
            assertThat(rightKeyValue.key.key()).isEqualTo("Doe");
            assertThat(rightKeyValue.key.window().startTime()).isEqualTo(
                "2000-01-01T01:06:01.00Z");
            assertThat(rightKeyValue.key.window().endTime()).isEqualTo(
                "2000-01-01T01:16:01.00Z");
            assertThat(rightKeyValue.value).isEqualTo(rightPerson);

            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        KafkaPerson leftPersonDoe = buildKafkaPerson("John", "Doe");
        leftInputTopic.pipeInput(
            new TestRecord<>("1", leftPersonDoe, Instant.parse("2000-01-01T01:00:00.00Z")));

        KafkaPerson leftPersonSmith = buildKafkaPerson("Jane", "Smith");
        leftInputTopic.pipeInput(
            new TestRecord<>("3", leftPersonSmith, Instant.parse("2000-01-01T01:10:30.00Z")));

        // At this point, the stream time is 01:10:30, so the first record expired in the
        // left state store (its window is 01:00:00->01:10:00).
        // However, the following delayed record will be joined with the first record
        // because of the grace period of 1 minute.

        KafkaPerson rightPersonDoe = buildKafkaPerson("Daniel", "Doe");
        rightInputTopic.pipeInput(
            new TestRecord<>("2", rightPersonDoe, Instant.parse("2000-01-01T01:05:00.00Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        // No record match before the end of the window, so the first record is emitted with a
        // null right person at the end of the window (+ grace period).
        assertThat(results.get(0).key).isEqualTo("Doe");
        assertThat(results.get(0).value.getPersonOne()).isEqualTo(leftPersonDoe);
        assertThat(results.get(0).value.getPersonTwo()).isNull();

        // The delayed record finally comes and is joined with the first record,
        // so an updated record is emitted with the right person.
        assertThat(results.get(1).key).isEqualTo("Doe");
        assertThat(results.get(1).value.getPersonOne()).isEqualTo(leftPersonDoe);
        assertThat(results.get(1).value.getPersonTwo()).isEqualTo(rightPersonDoe);

        // Check state store
        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(
            PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> leftKeyValueDoe00To10 = iterator.next();
            assertThat(leftKeyValueDoe00To10.key.key()).isEqualTo("Doe");
            assertThat(leftKeyValueDoe00To10.key.window().startTime()).isEqualTo(
                "2000-01-01T01:00:00.00Z");
            assertThat(leftKeyValueDoe00To10.key.window().endTime()).isEqualTo(
                "2000-01-01T01:10:00.00Z");
            assertThat(leftKeyValueDoe00To10.value).isEqualTo(leftPersonDoe);

            KeyValue<Windowed<String>, KafkaPerson> leftKeyValueSmith10m30To20m30 = iterator.next();
            assertThat(leftKeyValueSmith10m30To20m30.key.key()).isEqualTo("Smith");
            assertThat(leftKeyValueSmith10m30To20m30.key.window().startTime()).isEqualTo(
                "2000-01-01T01:10:30.00Z");
            assertThat(leftKeyValueSmith10m30To20m30.key.window().endTime()).isEqualTo(
                "2000-01-01T01:20:30.00Z");
            assertThat(leftKeyValueSmith10m30To20m30.value).isEqualTo(leftPersonSmith);

            assertThat(iterator.hasNext()).isFalse();
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(
            PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> rightKeyValue = iterator.next();
            assertThat(rightKeyValue.key.key()).isEqualTo("Doe");
            assertThat(rightKeyValue.key.window().startTime()).isEqualTo(
                "2000-01-01T01:05:00.00Z");
            assertThat(rightKeyValue.key.window().endTime()).isEqualTo(
                "2000-01-01T01:15:00.00Z");
            assertThat(rightKeyValue.value).isEqualTo(rightPersonDoe);

            assertThat(iterator.hasNext()).isFalse();
        }
    }

    private KafkaPerson buildKafkaPerson(String firstName, String lastName) {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName(firstName)
            .setLastName(lastName)
            .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
            .build();
    }
}
