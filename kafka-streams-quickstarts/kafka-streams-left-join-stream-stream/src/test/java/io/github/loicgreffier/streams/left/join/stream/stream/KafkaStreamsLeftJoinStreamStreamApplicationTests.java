package io.github.loicgreffier.streams.left.join.stream.stream;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaJoinPersons;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.left.join.stream.stream.app.KafkaStreamsTopology;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.github.loicgreffier.streams.left.join.stream.stream.constants.StateStore.PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE;
import static io.github.loicgreffier.streams.left.join.stream.stream.constants.Topic.*;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsLeftJoinStreamStreamApplicationTests {
    private static final String SCHEMA_REGISTRY_SCOPE = KafkaStreamsLeftJoinStreamStreamApplicationTests.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

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
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-left-join-stream-stream-test");
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
        Serde<KafkaJoinPersons> joinPersonsSerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        joinPersonsSerde.configure(config, false);

        leftInputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        rightInputTopic = testDriver.createInputTopic(PERSON_TOPIC_TWO, new StringSerializer(), personSerde.serializer());
        rekeyLeftOutputTopic = testDriver.createOutputTopic("streams-left-join-stream-stream-test-" + PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC + "-left-repartition", new StringDeserializer(),
                personSerde.deserializer());
        rekeyRightOutputTopic = testDriver.createOutputTopic("streams-left-join-stream-stream-test-" + PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC + "-right-repartition", new StringDeserializer(),
                personSerde.deserializer());
        joinOutputTopic = testDriver.createOutputTopic(PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC, new StringDeserializer(),
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
        KafkaPerson personLeft = buildKafkaPerson("Aaran", "Allen");
        KafkaPerson personRight = buildKafkaPerson("Brendan", "Allen");

        leftInputTopic.pipeInput("1", personLeft);
        rightInputTopic.pipeInput("2", personRight);

        List<KeyValue<String, KafkaPerson>> topicOneResults = rekeyLeftOutputTopic.readKeyValuesToList();
        List<KeyValue<String, KafkaPerson>> topicTwoResults = rekeyRightOutputTopic.readKeyValuesToList();

        assertThat(topicOneResults.get(0)).isEqualTo(KeyValue.pair("Allen", personLeft));
        assertThat(topicTwoResults.get(0)).isEqualTo(KeyValue.pair("Allen", personRight));
    }

    @Test
    void shouldJoinWhenTimeWindowIsRespected() {
        KafkaPerson personLeft1 = buildKafkaPerson("Aaran", "Allen");
        KafkaPerson personLeft2 = buildKafkaPerson("Bret", "Wise");
        KafkaPerson personRight1 = buildKafkaPerson("Brendan", "Allen");
        KafkaPerson personRight2 = buildKafkaPerson("Jude", "Wise");

        leftInputTopic.pipeInput(new TestRecord<>("1", personLeft1, Instant.parse("2000-01-01T01:00:00.00Z")));
        leftInputTopic.pipeInput(new TestRecord<>("2", personLeft2, Instant.parse("2000-01-01T01:01:00.00Z")));
        rightInputTopic.pipeInput(new TestRecord<>("3", personRight1, Instant.parse("2000-01-01T01:01:30.00Z")));
        rightInputTopic.pipeInput(new TestRecord<>("4", personRight2, Instant.parse("2000-01-01T01:02:00.00Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Allen");
        assertThat(results.get(0).value.getPersonOne()).isEqualTo(personLeft1);
        assertThat(results.get(0).value.getPersonTwo()).isEqualTo(personRight1);

        assertThat(results.get(1).key).isEqualTo("Wise");
        assertThat(results.get(1).value.getPersonOne()).isEqualTo(personLeft2);
        assertThat(results.get(1).value.getPersonTwo()).isEqualTo(personRight2);

        // Explore state store
        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> windowAllen01h00_01h10 = iterator.next();
            assertThat(windowAllen01h00_01h10.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h00_01h10.key.window().startTime()).isEqualTo("2000-01-01T01:00:00.00Z");
            assertThat(windowAllen01h00_01h10.key.window().endTime()).isEqualTo("2000-01-01T01:10:00.00Z");
            assertThat(windowAllen01h00_01h10.value).isEqualTo(personLeft1);

            KeyValue<Windowed<String>, KafkaPerson> windowWise01h01_01h11 = iterator.next();
            assertThat(windowWise01h01_01h11.key.key()).isEqualTo("Wise");
            assertThat(windowWise01h01_01h11.key.window().startTime()).isEqualTo("2000-01-01T01:01:00.00Z");
            assertThat(windowWise01h01_01h11.key.window().endTime()).isEqualTo("2000-01-01T01:11:00.00Z");
            assertThat(windowWise01h01_01h11.value).isEqualTo(personLeft2);

            assertThat(iterator.hasNext()).isFalse();
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> windowAllen01h01m30_01h11m30 = iterator.next();
            assertThat(windowAllen01h01m30_01h11m30.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h01m30_01h11m30.key.window().startTime()).isEqualTo("2000-01-01T01:01:30.00Z");
            assertThat(windowAllen01h01m30_01h11m30.key.window().endTime()).isEqualTo("2000-01-01T01:11:30.00Z");
            assertThat(windowAllen01h01m30_01h11m30.value).isEqualTo(personRight1);

            KeyValue<Windowed<String>, KafkaPerson> windowWise01h02_01h12 = iterator.next();
            assertThat(windowWise01h02_01h12.key.key()).isEqualTo("Wise");
            assertThat(windowWise01h02_01h12.key.window().startTime()).isEqualTo("2000-01-01T01:02:00.00Z");
            assertThat(windowWise01h02_01h12.key.window().endTime()).isEqualTo("2000-01-01T01:12:00.00Z");
            assertThat(windowWise01h02_01h12.value).isEqualTo(personRight2);

            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void shouldEmitLeftPersonWhenTimeWindowIsNotRespected() {
        KafkaPerson personLeft = buildKafkaPerson("Aaran", "Allen");
        KafkaPerson personRight = buildKafkaPerson("Brendan", "Allen");

        leftInputTopic.pipeInput(new TestRecord<>("1", personLeft, Instant.parse("2000-01-01T01:00:00.00Z")));
        // The default value [left record;null] is emitted after window end + grace period (6 minutes)
        rightInputTopic.pipeInput(new TestRecord<>("2", personRight, Instant.parse("2000-01-01T01:06:01.00Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Allen");
        assertThat(results.get(0).value.getPersonOne()).isEqualTo(personLeft);
        assertThat(results.get(0).value.getPersonTwo()).isNull();

        // Explore state store
        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> storedPersonLeftOne = iterator.next();
            assertThat(storedPersonLeftOne.key.key()).isEqualTo("Allen");
            assertThat(storedPersonLeftOne.key.window().startTime()).isEqualTo("2000-01-01T01:00:00.00Z");
            assertThat(storedPersonLeftOne.key.window().endTime()).isEqualTo("2000-01-01T01:10:00.00Z");
            assertThat(storedPersonLeftOne.value).isEqualTo(personLeft);
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> storedPersonRightOne = iterator.next();
            assertThat(storedPersonRightOne.key.key()).isEqualTo("Allen");
            assertThat(storedPersonRightOne.key.window().startTime()).isEqualTo("2000-01-01T01:06:01.00Z");
            assertThat(storedPersonRightOne.key.window().endTime()).isEqualTo("2000-01-01T01:16:01.00Z");
            assertThat(storedPersonRightOne.value).isEqualTo(personRight);
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        KafkaPerson personLeft1 = buildKafkaPerson("Aaran", "Allen");
        KafkaPerson personLeft2 = buildKafkaPerson("Bret", "Wise");
        KafkaPerson personRight1 = buildKafkaPerson("Jude", "Allen");

        leftInputTopic.pipeInput(new TestRecord<>("1", personLeft1, Instant.parse("2000-01-01T01:00:00.00Z")));
        leftInputTopic.pipeInput(new TestRecord<>("3", personLeft2, Instant.parse("2000-01-01T01:10:30.00Z")));

        // At this point, the stream time (01:10:30) is after the end of the first record window (Allen@01:00:00->01:10:00)
        // But, this delayed record will match the first record window thanks to grace period that keeps window opened for 1 more minute
        rightInputTopic.pipeInput(new TestRecord<>("2", personRight1, Instant.parse("2000-01-01T01:05:00.00Z")));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        // No record match when window ends
        assertThat(results.get(0).key).isEqualTo("Allen");
        assertThat(results.get(0).value.getPersonOne()).isEqualTo(personLeft1);
        assertThat(results.get(0).value.getPersonTwo()).isNull();

        // Delayed record finally came
        assertThat(results.get(1).key).isEqualTo("Allen");
        assertThat(results.get(1).value.getPersonOne()).isEqualTo(personLeft1);
        assertThat(results.get(1).value.getPersonTwo()).isEqualTo(personRight1);

        // Explore state store
        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> windowAllen01h00_01h10 = iterator.next();
            assertThat(windowAllen01h00_01h10.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h00_01h10.key.window().startTime()).isEqualTo("2000-01-01T01:00:00.00Z");
            assertThat(windowAllen01h00_01h10.key.window().endTime()).isEqualTo("2000-01-01T01:10:00.00Z");
            assertThat(windowAllen01h00_01h10.value).isEqualTo(personLeft1);

            KeyValue<Windowed<String>, KafkaPerson> windowAllen01h10_01h20 = iterator.next();
            assertThat(windowAllen01h10_01h20.key.key()).isEqualTo("Wise");
            assertThat(windowAllen01h10_01h20.key.window().startTime()).isEqualTo("2000-01-01T01:10:30.00Z");
            assertThat(windowAllen01h10_01h20.key.window().endTime()).isEqualTo("2000-01-01T01:20:30.00Z");
            assertThat(windowAllen01h10_01h20.value).isEqualTo(personLeft2);

            assertThat(iterator.hasNext()).isFalse();
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> windowAllen01h03_01h13 = iterator.next();
            assertThat(windowAllen01h03_01h13.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h03_01h13.key.window().startTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(windowAllen01h03_01h13.key.window().endTime()).isEqualTo("2000-01-01T01:15:00.00Z");
            assertThat(windowAllen01h03_01h13.value).isEqualTo(personRight1);

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
