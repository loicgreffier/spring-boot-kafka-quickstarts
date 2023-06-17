package io.github.loicgreffier.streams.aggregate.hopping.window;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.hopping.window.app.KafkaStreamsAggregateHoppingWindowTopology;
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

import static io.github.loicgreffier.streams.aggregate.hopping.window.constants.StateStore.PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constants.Topic.PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsAggregateHoppingWindowTest {
    private static final String SCHEMA_REGISTRY_SCOPE = KafkaStreamsAggregateHoppingWindowTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPersonGroup> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-aggregate-hopping-window-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsAggregateHoppingWindowTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        // Create Serde for input and output topics
        Serde<KafkaPerson> personSerde = new SpecificAvroSerde<>();
        Serde<KafkaPersonGroup> personGroupSerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        personGroupSerde.configure(config, false);

        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC, new StringDeserializer(), personGroupSerde.deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldAggregateFirstNamesByLastNameWhenTimeWindowIsRespected() {
        KafkaPerson personOne = buildKafkaPersonValue("Aaran", "Abbott");
        KafkaPerson personTwo = buildKafkaPersonValue("Brendan", "Abbott");
        KafkaPerson personThree = buildKafkaPersonValue("Bret", "Holman");
        KafkaPerson personFour = buildKafkaPersonValue("Daimhin", "Abbott");
        KafkaPerson personFive = buildKafkaPersonValue("Jiao", "Patton");
        KafkaPerson personSix = buildKafkaPersonValue("Jude", "Holman");

        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopic.pipeInput(new TestRecord<>("1", personOne, start));
        inputTopic.pipeInput(new TestRecord<>("2", personTwo, start.plus(1, ChronoUnit.MINUTES)));
        inputTopic.pipeInput(new TestRecord<>("3", personThree, start.plus(1, ChronoUnit.MINUTES).plusSeconds(30)));
        inputTopic.pipeInput(new TestRecord<>("4", personFour, start.plus(2, ChronoUnit.MINUTES)));
        inputTopic.pipeInput(new TestRecord<>("5", personFive, start.plus(3, ChronoUnit.MINUTES)));
        inputTopic.pipeInput(new TestRecord<>("6", personSix, start.plus(4, ChronoUnit.MINUTES)));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(15);
        assertThat(results.get(0).key).isEqualTo("Abbott@2000-01-01T00:56:00Z->2000-01-01T01:01:00Z");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

        assertThat(results.get(1).key).isEqualTo("Abbott@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

        assertThat(results.get(2).key).isEqualTo("Abbott@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

        assertThat(results.get(3).key).isEqualTo("Abbott@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan");

        assertThat(results.get(4).key).isEqualTo("Abbott@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(4).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan");

        assertThat(results.get(5).key).isEqualTo("Holman@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z");
        assertThat(results.get(5).value.getFirstNameByLastName().get("Holman")).containsExactly("Bret");

        assertThat(results.get(6).key).isEqualTo("Holman@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(6).value.getFirstNameByLastName().get("Holman")).containsExactly("Bret");

        assertThat(results.get(7).key).isEqualTo("Abbott@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z");
        assertThat(results.get(7).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan", "Daimhin");

        assertThat(results.get(8).key).isEqualTo("Abbott@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(8).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan", "Daimhin");

        assertThat(results.get(9).key).isEqualTo("Abbott@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z");
        assertThat(results.get(9).value.getFirstNameByLastName().get("Abbott")).containsExactly("Daimhin");

        assertThat(results.get(10).key).isEqualTo("Patton@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(10).value.getFirstNameByLastName().get("Patton")).containsExactly("Jiao");

        assertThat(results.get(11).key).isEqualTo("Patton@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z");
        assertThat(results.get(11).value.getFirstNameByLastName().get("Patton")).containsExactly("Jiao");

        assertThat(results.get(12).key).isEqualTo("Holman@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(12).value.getFirstNameByLastName().get("Holman")).containsExactly("Bret", "Jude");

        assertThat(results.get(13).key).isEqualTo("Holman@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z");
        assertThat(results.get(13).value.getFirstNameByLastName().get("Holman")).containsExactly("Jude");

        assertThat(results.get(14).key).isEqualTo("Holman@2000-01-01T01:04:00Z->2000-01-01T01:09:00Z");
        assertThat(results.get(14).value.getFirstNameByLastName().get("Holman")).containsExactly("Jude");

        WindowStore<String, KafkaPersonGroup> stateStore = testDriver.getWindowStore(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> firstWindow = iterator.next();
            assertThat(firstWindow.key.key()).isEqualTo("Abbott");
            assertThat(firstWindow.key.window().startTime()).isEqualTo("2000-01-01T00:56:00Z");
            assertThat(firstWindow.key.window().endTime()).isEqualTo("2000-01-01T01:01:00.00Z");
            assertThat(firstWindow.value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

            KeyValue<Windowed<String>, KafkaPersonGroup> secondWindowAbbott = iterator.next();
            assertThat(secondWindowAbbott.key.key()).isEqualTo("Abbott");
            assertThat(secondWindowAbbott.key.window().startTime()).isEqualTo("2000-01-01T00:58:00Z");
            assertThat(secondWindowAbbott.key.window().endTime()).isEqualTo("2000-01-01T01:03:00.00Z");
            assertThat(secondWindowAbbott.value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan", "Daimhin");

            KeyValue<Windowed<String>, KafkaPersonGroup> secondWindowHolman = iterator.next();
            assertThat(secondWindowHolman.key.key()).isEqualTo("Holman");
            assertThat(secondWindowHolman.key.window().startTime()).isEqualTo("2000-01-01T00:58:00Z");
            assertThat(secondWindowHolman.key.window().endTime()).isEqualTo("2000-01-01T01:03:00.00Z");
            assertThat(secondWindowHolman.value.getFirstNameByLastName().get("Holman")).containsExactly("Bret");

            KeyValue<Windowed<String>, KafkaPersonGroup> thirdWindowAbbott = iterator.next();
            assertThat(thirdWindowAbbott.key.key()).isEqualTo("Abbott");
            assertThat(thirdWindowAbbott.key.window().startTime()).isEqualTo("2000-01-01T01:00:00Z");
            assertThat(thirdWindowAbbott.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(thirdWindowAbbott.value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan", "Daimhin");

            KeyValue<Windowed<String>, KafkaPersonGroup> fourthWindowAbbott = iterator.next();
            assertThat(fourthWindowAbbott.key.key()).isEqualTo("Abbott");
            assertThat(fourthWindowAbbott.key.window().startTime()).isEqualTo("2000-01-01T01:02:00Z");
            assertThat(fourthWindowAbbott.key.window().endTime()).isEqualTo("2000-01-01T01:07:00.00Z");
            assertThat(fourthWindowAbbott.value.getFirstNameByLastName().get("Abbott")).containsExactly("Daimhin");

            KeyValue<Windowed<String>, KafkaPersonGroup> thirdWindowHolman = iterator.next();
            assertThat(thirdWindowHolman.key.key()).isEqualTo("Holman");
            assertThat(thirdWindowHolman.key.window().startTime()).isEqualTo("2000-01-01T01:00:00Z");
            assertThat(thirdWindowHolman.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(thirdWindowHolman.value.getFirstNameByLastName().get("Holman")).containsExactly("Bret", "Jude");

            KeyValue<Windowed<String>, KafkaPersonGroup> fourthWindowHolman = iterator.next();
            assertThat(fourthWindowHolman.key.key()).isEqualTo("Holman");
            assertThat(fourthWindowHolman.key.window().startTime()).isEqualTo("2000-01-01T01:02:00Z");
            assertThat(fourthWindowHolman.key.window().endTime()).isEqualTo("2000-01-01T01:07:00.00Z");
            assertThat(fourthWindowHolman.value.getFirstNameByLastName().get("Holman")).containsExactly("Jude");

            KeyValue<Windowed<String>, KafkaPersonGroup> thirdWindowPatton = iterator.next();
            assertThat(thirdWindowPatton.key.key()).isEqualTo("Patton");
            assertThat(thirdWindowPatton.key.window().startTime()).isEqualTo("2000-01-01T01:00:00Z");
            assertThat(thirdWindowPatton.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(thirdWindowPatton.value.getFirstNameByLastName().get("Patton")).containsExactly("Jiao");

            KeyValue<Windowed<String>, KafkaPersonGroup> fourthWindowPatton = iterator.next();
            assertThat(fourthWindowPatton.key.key()).isEqualTo("Patton");
            assertThat(fourthWindowPatton.key.window().startTime()).isEqualTo("2000-01-01T01:02:00Z");
            assertThat(fourthWindowPatton.key.window().endTime()).isEqualTo("2000-01-01T01:07:00.00Z");
            assertThat(fourthWindowPatton.value.getFirstNameByLastName().get("Patton")).containsExactly("Jiao");

            KeyValue<Windowed<String>, KafkaPersonGroup> fifthWindowHolman = iterator.next();
            assertThat(fifthWindowHolman.key.key()).isEqualTo("Holman");
            assertThat(fifthWindowHolman.key.window().startTime()).isEqualTo("2000-01-01T01:04:00Z");
            assertThat(fifthWindowHolman.key.window().endTime()).isEqualTo("2000-01-01T01:09:00.00Z");
            assertThat(fifthWindowHolman.value.getFirstNameByLastName().get("Holman")).containsExactly("Jude");

            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void shouldNotAggregateFirstNamesByLastNameWhenTimeWindowIsNotRespected() {
        KafkaPerson personOne = buildKafkaPersonValue("Aaran", "Abbott");
        KafkaPerson personTwo = buildKafkaPersonValue("Brendan", "Abbott");

        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopic.pipeInput(new TestRecord<>("1", personOne, start));
        inputTopic.pipeInput(new TestRecord<>("2", personTwo, start.plus(5, ChronoUnit.MINUTES)));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(5);
        assertThat(results.get(0).key).isEqualTo("Abbott@2000-01-01T00:56:00Z->2000-01-01T01:01:00Z");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

        assertThat(results.get(1).key).isEqualTo("Abbott@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

        assertThat(results.get(2).key).isEqualTo("Abbott@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

        assertThat(results.get(3).key).isEqualTo("Abbott@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Abbott")).containsExactly("Brendan");

        assertThat(results.get(4).key).isEqualTo("Abbott@2000-01-01T01:04:00Z->2000-01-01T01:09:00Z");
        assertThat(results.get(4).value.getFirstNameByLastName().get("Abbott")).containsExactly("Brendan");

        WindowStore<String, KafkaPersonGroup> stateStore = testDriver.getWindowStore(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> firstWindow = iterator.next();
            assertThat(firstWindow.key.key()).isEqualTo("Abbott");
            assertThat(firstWindow.key.window().startTime()).isEqualTo("2000-01-01T00:56:00Z");
            assertThat(firstWindow.key.window().endTime()).isEqualTo("2000-01-01T01:01:00.00Z");
            assertThat(firstWindow.value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

            KeyValue<Windowed<String>, KafkaPersonGroup> secondWindow = iterator.next();
            assertThat(secondWindow.key.key()).isEqualTo("Abbott");
            assertThat(secondWindow.key.window().startTime()).isEqualTo("2000-01-01T00:58:00Z");
            assertThat(secondWindow.key.window().endTime()).isEqualTo("2000-01-01T01:03:00.00Z");
            assertThat(secondWindow.value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

            KeyValue<Windowed<String>, KafkaPersonGroup> thirdWindow = iterator.next();
            assertThat(thirdWindow.key.key()).isEqualTo("Abbott");
            assertThat(thirdWindow.key.window().startTime()).isEqualTo("2000-01-01T01:00:00Z");
            assertThat(thirdWindow.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(thirdWindow.value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

            KeyValue<Windowed<String>, KafkaPersonGroup> fourthWindow = iterator.next();
            assertThat(fourthWindow.key.key()).isEqualTo("Abbott");
            assertThat(fourthWindow.key.window().startTime()).isEqualTo("2000-01-01T01:02:00Z");
            assertThat(fourthWindow.key.window().endTime()).isEqualTo("2000-01-01T01:07:00.00Z");
            assertThat(fourthWindow.value.getFirstNameByLastName().get("Abbott")).containsExactly("Brendan");

            KeyValue<Windowed<String>, KafkaPersonGroup> fifthWindow = iterator.next();
            assertThat(fifthWindow.key.key()).isEqualTo("Abbott");
            assertThat(fifthWindow.key.window().startTime()).isEqualTo("2000-01-01T01:04:00Z");
            assertThat(fifthWindow.key.window().endTime()).isEqualTo("2000-01-01T01:09:00.00Z");
            assertThat(fifthWindow.value.getFirstNameByLastName().get("Abbott")).containsExactly("Brendan");

            assertThat(iterator.hasNext()).isFalse();
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
