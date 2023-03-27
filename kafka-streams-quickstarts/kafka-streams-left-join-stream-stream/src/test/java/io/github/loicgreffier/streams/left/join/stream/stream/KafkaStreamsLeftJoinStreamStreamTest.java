package io.github.loicgreffier.streams.left.join.stream.stream;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.github.loicgreffier.streams.left.join.stream.stream.constants.StateStore;
import io.github.loicgreffier.streams.left.join.stream.stream.constants.Topic;
import io.github.loicgreffier.avro.KafkaJoinPersons;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.left.join.stream.stream.app.KafkaStreamsLeftJoinStreamStreamTopology;
import io.github.loicgreffier.streams.left.join.stream.stream.serdes.CustomSerdes;
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

import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsLeftJoinStreamStreamTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopicOne;
    private TestInputTopic<String, KafkaPerson> inputTopicTwo;
    private TestOutputTopic<String, KafkaPerson> rekeyInputTopicOne;
    private TestOutputTopic<String, KafkaPerson> rekeyInputTopicTwo;
    private TestOutputTopic<String, KafkaJoinPersons> joinOutputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-left-join-stream-stream-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" +  getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + getClass().getName());
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsLeftJoinStreamStreamTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        inputTopicOne = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        inputTopicTwo = testDriver.createInputTopic(Topic.PERSON_TOPIC_TWO.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        rekeyInputTopicOne = testDriver.createOutputTopic("streams-left-join-stream-stream-test-" + Topic.PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC + "-left-repartition", new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());

        rekeyInputTopicTwo = testDriver.createOutputTopic("streams-left-join-stream-stream-test-" + Topic.PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC + "-right-repartition", new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());

        joinOutputTopic = testDriver.createOutputTopic(Topic.PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaJoinPersons>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope("mock://" + getClass().getName());
    }

    @Test
    void shouldRekey() {
        KafkaPerson personLeft = buildKafkaPersonValue("Callie", "Acosta");
        KafkaPerson personRight = buildKafkaPersonValue("Finnley", "Acosta");

        inputTopicOne.pipeInput("1", personLeft);
        inputTopicTwo.pipeInput("2", personRight);

        List<KeyValue<String, KafkaPerson>> topicOneResults = rekeyInputTopicOne.readKeyValuesToList();
        List<KeyValue<String, KafkaPerson>> topicTwoResults = rekeyInputTopicTwo.readKeyValuesToList();

        assertThat(topicOneResults).hasSize(1);
        assertThat(topicOneResults.get(0)).isEqualTo(KeyValue.pair("Acosta", personLeft));

        assertThat(topicTwoResults).hasSize(1);
        assertThat(topicTwoResults.get(0)).isEqualTo(KeyValue.pair("Acosta", personRight));
    }

    @Test
    void shouldJoinWhenTimeWindowIsRespected() {
        KafkaPerson personLeftOne = buildKafkaPersonValue("Callie", "Acosta");
        KafkaPerson personLeftTwo = buildKafkaPersonValue("Oscar", "Rhodes");
        KafkaPerson personRightOne = buildKafkaPersonValue("Tyra", "Acosta");
        KafkaPerson personRightTwo = buildKafkaPersonValue("Finnlay", "Rhodes");
        KafkaPerson personRightThree = buildKafkaPersonValue("Robby", "Acosta");

        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopicOne.pipeInput(new TestRecord<>("1", personLeftOne, start));
        inputTopicTwo.pipeInput(new TestRecord<>("2", personRightOne, start.plus(1, ChronoUnit.MINUTES)));
        inputTopicTwo.pipeInput(new TestRecord<>("3", personRightTwo, start.plus(1, ChronoUnit.MINUTES).plusSeconds(30)));
        inputTopicTwo.pipeInput(new TestRecord<>("4", personRightThree, start.plus(2, ChronoUnit.MINUTES)));
        inputTopicOne.pipeInput(new TestRecord<>("5", personLeftTwo, start.plus(3, ChronoUnit.MINUTES)));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(3);
        assertThat(results.get(0)).isEqualTo(KeyValue.pair("Acosta", KafkaJoinPersons.newBuilder()
                .setPersonOne(personLeftOne)
                .setPersonTwo(personRightOne)
                .build()));

        assertThat(results.get(1)).isEqualTo(KeyValue.pair("Acosta", KafkaJoinPersons.newBuilder()
                .setPersonOne(personLeftOne)
                .setPersonTwo(personRightThree)
                .build()));

        assertThat(results.get(2)).isEqualTo(KeyValue.pair("Rhodes", KafkaJoinPersons.newBuilder()
                .setPersonOne(personLeftTwo)
                .setPersonTwo(personRightTwo)
                .build()));

        // Test state stores content
        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(StateStore.PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> storedPersonLeftOne = iterator.next();
            assertThat(storedPersonLeftOne.key.key()).isEqualTo("Acosta");
            assertThat(storedPersonLeftOne.key.window().startTime()).isEqualTo("2000-01-01T01:00:00.00Z");
            assertThat(storedPersonLeftOne.key.window().endTime()).isEqualTo("2000-01-01T01:04:00.00Z");
            assertThat(storedPersonLeftOne.value).isEqualTo(personLeftOne);

            KeyValue<Windowed<String>, KafkaPerson> storedPersonLeftTwo = iterator.next();
            assertThat(storedPersonLeftTwo.key.key()).isEqualTo("Rhodes");
            assertThat(storedPersonLeftTwo.key.window().startTime()).isEqualTo("2000-01-01T01:03:00.00Z");
            assertThat(storedPersonLeftTwo.key.window().endTime()).isEqualTo("2000-01-01T01:07:00.00Z");
            assertThat(storedPersonLeftTwo.value).isEqualTo(personLeftTwo);
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(StateStore.PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> storedPersonRightOne = iterator.next();
            assertThat(storedPersonRightOne.key.key()).isEqualTo("Acosta");
            assertThat(storedPersonRightOne.key.window().startTime()).isEqualTo("2000-01-01T01:01:00.00Z");
            assertThat(storedPersonRightOne.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(storedPersonRightOne.value).isEqualTo(personRightOne);

            KeyValue<Windowed<String>, KafkaPerson> storedPersonRightThree = iterator.next();
            assertThat(storedPersonRightThree.key.key()).isEqualTo("Acosta");
            assertThat(storedPersonRightThree.key.window().startTime()).isEqualTo("2000-01-01T01:02:00.00Z");
            assertThat(storedPersonRightThree.key.window().endTime()).isEqualTo("2000-01-01T01:06:00.00Z");
            assertThat(storedPersonRightThree.value).isEqualTo(personRightThree);

            KeyValue<Windowed<String>, KafkaPerson> storedPersonRightTwo = iterator.next();
            assertThat(storedPersonRightTwo.key.key()).isEqualTo("Rhodes");
            assertThat(storedPersonRightTwo.key.window().startTime()).isEqualTo("2000-01-01T01:01:30.00Z");
            assertThat(storedPersonRightTwo.key.window().endTime()).isEqualTo("2000-01-01T01:05:30.00Z");
            assertThat(storedPersonRightTwo.value).isEqualTo(personRightTwo);
        }
    }

    @Test
    void shouldEmitLeftPersonWhenTimeWindowIsNotRespected() {
        KafkaPerson personLeftOne = buildKafkaPersonValue("Callie", "Acosta");
        KafkaPerson personRightOne = buildKafkaPersonValue("Zubayr", "Acosta");

        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopicOne.pipeInput(new TestRecord<>("1", personLeftOne, start));
        inputTopicTwo.pipeInput(new TestRecord<>("2", personRightOne, start.plus(3, ChronoUnit.MINUTES)));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isEqualTo(KeyValue.pair("Acosta", KafkaJoinPersons.newBuilder()
                .setPersonOne(personLeftOne)
                .setPersonTwo(null)
                .build()));

        // Test state stores content
        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(StateStore.PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> storedPersonLeftOne = iterator.next();
            assertThat(storedPersonLeftOne.key.key()).isEqualTo("Acosta");
            assertThat(storedPersonLeftOne.key.window().startTime()).isEqualTo("2000-01-01T01:00:00.00Z");
            assertThat(storedPersonLeftOne.key.window().endTime()).isEqualTo("2000-01-01T01:04:00.00Z");
            assertThat(storedPersonLeftOne.value).isEqualTo(personLeftOne);
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(StateStore.PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> storedPersonRightOne = iterator.next();
            assertThat(storedPersonRightOne.key.key()).isEqualTo("Acosta");
            assertThat(storedPersonRightOne.key.window().startTime()).isEqualTo("2000-01-01T01:03:00.00Z");
            assertThat(storedPersonRightOne.key.window().endTime()).isEqualTo("2000-01-01T01:07:00.00Z");
            assertThat(storedPersonRightOne.value).isEqualTo(personRightOne);
        }
    }

    @Test
    void shouldEmitLeftPersonWhenNoMatchingValue() {
        KafkaPerson personLeftOne = buildKafkaPersonValue("Callie", "Acosta");
        KafkaPerson personRightOne = buildKafkaPersonValue("Zubayr", "Rhodes");

        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopicOne.pipeInput(new TestRecord<>("1", personLeftOne, start));
        inputTopicTwo.pipeInput(new TestRecord<>("2", personRightOne, start.plus(3, ChronoUnit.MINUTES)));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isEqualTo(KeyValue.pair("Acosta", KafkaJoinPersons.newBuilder()
                .setPersonOne(personLeftOne)
                .setPersonTwo(null)
                .build()));

        // Test state stores content
        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(StateStore.PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> storedPersonLeftOne = iterator.next();
            assertThat(storedPersonLeftOne.key.key()).isEqualTo("Acosta");
            assertThat(storedPersonLeftOne.key.window().startTime()).isEqualTo("2000-01-01T01:00:00.00Z");
            assertThat(storedPersonLeftOne.key.window().endTime()).isEqualTo("2000-01-01T01:04:00.00Z");
            assertThat(storedPersonLeftOne.value).isEqualTo(personLeftOne);
        }

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(StateStore.PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE + "-outer-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> storedPersonRightOne = iterator.next();
            assertThat(storedPersonRightOne.key.key()).isEqualTo("Rhodes");
            assertThat(storedPersonRightOne.key.window().startTime()).isEqualTo("2000-01-01T01:03:00.00Z");
            assertThat(storedPersonRightOne.key.window().endTime()).isEqualTo("2000-01-01T01:07:00.00Z");
            assertThat(storedPersonRightOne.value).isEqualTo(personRightOne);
        }
    }

    private KafkaPerson buildKafkaPersonValue(String firstName, String lastName) {
        return KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName(firstName)
                .setLastName(lastName)
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build();
    }
}
