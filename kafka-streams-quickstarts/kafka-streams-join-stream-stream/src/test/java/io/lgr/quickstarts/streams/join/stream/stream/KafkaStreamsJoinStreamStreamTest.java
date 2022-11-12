package io.lgr.quickstarts.streams.join.stream.stream;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.lgr.quickstarts.avro.KafkaJoinPersons;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.join.stream.stream.app.KafkaStreamsJoinStreamStreamTopology;
import io.lgr.quickstarts.streams.join.stream.stream.constants.StateStore;
import io.lgr.quickstarts.streams.join.stream.stream.constants.Topic;
import io.lgr.quickstarts.streams.join.stream.stream.serdes.CustomSerdes;
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

class KafkaStreamsJoinStreamStreamTest {
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
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-join-stream-stream-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" +  getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsJoinStreamStreamTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.ofEpochMilli(1577836800000L));

        inputTopicOne = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        inputTopicTwo = testDriver.createInputTopic(Topic.PERSON_TOPIC_TWO.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        rekeyInputTopicOne = testDriver.createOutputTopic("streams-join-stream-stream-test-" + Topic.PERSON_JOIN_STREAM_STREAM_REKEY_TOPIC + "-left-repartition", new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());

        rekeyInputTopicTwo = testDriver.createOutputTopic("streams-join-stream-stream-test-" + Topic.PERSON_JOIN_STREAM_STREAM_REKEY_TOPIC + "-right-repartition", new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());

        joinOutputTopic = testDriver.createOutputTopic(Topic.PERSON_JOIN_STREAM_STREAM_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaJoinPersons>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(getClass().getName());
    }

    @Test
    void shouldRekeyBothInputTopics() {
        inputTopicOne.pipeInput("1", buildKafkaPersonValue("Callie", "Acosta"));
        inputTopicTwo.pipeInput("2", buildKafkaPersonValue("Finnley", "Acosta"));
        List<KeyValue<String, KafkaPerson>> topicOneResults = rekeyInputTopicOne.readKeyValuesToList();
        List<KeyValue<String, KafkaPerson>> topicTwoResults = rekeyInputTopicTwo.readKeyValuesToList();

        assertThat(topicOneResults).hasSize(1);
        assertThat(topicOneResults.get(0).key).isEqualTo("Acosta");
        assertThat(topicOneResults.get(0).value.getFirstName()).isEqualTo("Callie");
        assertThat(topicOneResults.get(0).value.getLastName()).isEqualTo("Acosta");

        assertThat(topicTwoResults).hasSize(1);
        assertThat(topicTwoResults.get(0).key).isEqualTo("Acosta");
        assertThat(topicTwoResults.get(0).value.getFirstName()).isEqualTo("Finnley");
        assertThat(topicTwoResults.get(0).value.getLastName()).isEqualTo("Acosta");
    }

    @Test
    void shouldJoinWhenTimeWindowIsRespected() {
        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopicOne.pipeInput(new TestRecord<>("1", buildKafkaPersonValue("Callie", "Acosta"), start));
        inputTopicTwo.pipeInput(new TestRecord<>("2", buildKafkaPersonValue("Zubayr", "Acosta"), start
                .minus(2, ChronoUnit.MINUTES)));
        inputTopicTwo.pipeInput(new TestRecord<>("3", buildKafkaPersonValue("Tayyab", "Acosta"), start
                .minus(1, ChronoUnit.MINUTES)));
        inputTopicTwo.pipeInput(new TestRecord<>("4", buildKafkaPersonValue("Tyra", "Acosta"), start
                .plus(1, ChronoUnit.MINUTES)));
        inputTopicTwo.pipeInput(new TestRecord<>("5", buildKafkaPersonValue("Robby", "Acosta"), start
                .plus(2, ChronoUnit.MINUTES)));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(4);
        assertThat(results.get(0).key).isEqualTo("Acosta");
        assertThat(results.get(0).value.getPersonOne().getFirstName()).isEqualTo("Callie");
        assertThat(results.get(0).value.getPersonTwo().getFirstName()).isEqualTo("Zubayr");

        assertThat(results.get(1).key).isEqualTo("Acosta");
        assertThat(results.get(1).value.getPersonOne().getFirstName()).isEqualTo("Callie");
        assertThat(results.get(1).value.getPersonTwo().getFirstName()).isEqualTo("Tayyab");

        assertThat(results.get(2).key).isEqualTo("Acosta");
        assertThat(results.get(2).value.getPersonOne().getFirstName()).isEqualTo("Callie");
        assertThat(results.get(2).value.getPersonTwo().getFirstName()).isEqualTo("Tyra");

        assertThat(results.get(3).key).isEqualTo("Acosta");
        assertThat(results.get(3).value.getPersonOne().getFirstName()).isEqualTo("Callie");
        assertThat(results.get(3).value.getPersonTwo().getFirstName()).isEqualTo("Robby");

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(StateStore.PERSON_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Callie");
            assertThat(record.value.getLastName()).isEqualTo("Acosta");
        }

        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(StateStore.PERSON_JOIN_STREAM_STREAM_STATE_STORE + "-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Zubayr");
            assertThat(record.value.getLastName()).isEqualTo("Acosta");

            record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Tayyab");
            assertThat(record.value.getLastName()).isEqualTo("Acosta");

            record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Tyra");
            assertThat(record.value.getLastName()).isEqualTo("Acosta");

            record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Robby");
            assertThat(record.value.getLastName()).isEqualTo("Acosta");
        }
    }

    @Test
    void shouldJoinWhenTimeWindowIsRespectedMultipleJoins() {
        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopicOne.pipeInput(new TestRecord<>("1", buildKafkaPersonValue("Callie", "Acosta"), start));
        inputTopicTwo.pipeInput(new TestRecord<>("2", buildKafkaPersonValue("Tyra", "Acosta"), start
                .plus(1, ChronoUnit.MINUTES)));
        inputTopicTwo.pipeInput(new TestRecord<>("3", buildKafkaPersonValue("Finnlay", "Rhodes"), start
                .plus(1, ChronoUnit.MINUTES)
                .plusSeconds(30)));
        inputTopicTwo.pipeInput(new TestRecord<>("4", buildKafkaPersonValue("Robby", "Acosta"), start
                .plus(2, ChronoUnit.MINUTES)));
        inputTopicOne.pipeInput(new TestRecord<>("5", buildKafkaPersonValue("Oscar", "Rhodes"), start
                .plus(3, ChronoUnit.MINUTES)));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(3);
        assertThat(results.get(0).key).isEqualTo("Acosta");
        assertThat(results.get(0).value.getPersonOne().getFirstName()).isEqualTo("Callie");
        assertThat(results.get(0).value.getPersonTwo().getFirstName()).isEqualTo("Tyra");

        assertThat(results.get(1).key).isEqualTo("Acosta");
        assertThat(results.get(1).value.getPersonOne().getFirstName()).isEqualTo("Callie");
        assertThat(results.get(1).value.getPersonTwo().getFirstName()).isEqualTo("Robby");

        assertThat(results.get(2).key).isEqualTo("Rhodes");
        assertThat(results.get(2).value.getPersonOne().getFirstName()).isEqualTo("Oscar");
        assertThat(results.get(2).value.getPersonTwo().getFirstName()).isEqualTo("Finnlay");

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(StateStore.PERSON_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Callie");
            assertThat(record.value.getLastName()).isEqualTo("Acosta");

            record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Oscar");
            assertThat(record.value.getLastName()).isEqualTo("Rhodes");
        }

        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(StateStore.PERSON_JOIN_STREAM_STREAM_STATE_STORE + "-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Tyra");
            assertThat(record.value.getLastName()).isEqualTo("Acosta");

            record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Robby");
            assertThat(record.value.getLastName()).isEqualTo("Acosta");

            record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Finnlay");
            assertThat(record.value.getLastName()).isEqualTo("Rhodes");
        }
    }

    @Test
    void shouldNotJoinWhenTimeWindowIsNotRespected() {
        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopicOne.pipeInput(new TestRecord<>("1", buildKafkaPersonValue("Callie", "Acosta"), start));
        inputTopicTwo.pipeInput(new TestRecord<>("2", buildKafkaPersonValue("Zubayr", "Acosta"), start
                .minus(3, ChronoUnit.MINUTES)));
        inputTopicTwo.pipeInput(new TestRecord<>("3", buildKafkaPersonValue("Robby", "Acosta"), start
                .plus(3, ChronoUnit.MINUTES)));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).isEmpty();

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(StateStore.PERSON_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Callie");
            assertThat(record.value.getLastName()).isEqualTo("Acosta");
        }

        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(StateStore.PERSON_JOIN_STREAM_STREAM_STATE_STORE + "-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Robby");
            assertThat(record.value.getLastName()).isEqualTo("Acosta");
        }
    }

    @Test
    void shouldNotJoinWhenNoMatchingValue() {
        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopicOne.pipeInput(new TestRecord<>("1", buildKafkaPersonValue("Callie", "Acosta"), start));
        inputTopicTwo.pipeInput(new TestRecord<>("2", buildKafkaPersonValue("Zubayr", "Rhodes"), start));

        List<KeyValue<String, KafkaJoinPersons>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).isEmpty();

        WindowStore<String, KafkaPerson> rightStateStore = testDriver.getWindowStore(StateStore.PERSON_JOIN_STREAM_STREAM_STATE_STORE + "-this-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = rightStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Callie");
            assertThat(record.value.getLastName()).isEqualTo("Acosta");
        }

        WindowStore<String, KafkaPerson> leftStateStore = testDriver.getWindowStore(StateStore.PERSON_JOIN_STREAM_STREAM_STATE_STORE + "-other-join-store");
        try (KeyValueIterator<Windowed<String>, KafkaPerson> iterator = leftStateStore.all()) {
            KeyValue<Windowed<String>, KafkaPerson> record = iterator.next();
            assertThat(record.value.getFirstName()).isEqualTo("Zubayr");
            assertThat(record.value.getLastName()).isEqualTo("Rhodes");
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
