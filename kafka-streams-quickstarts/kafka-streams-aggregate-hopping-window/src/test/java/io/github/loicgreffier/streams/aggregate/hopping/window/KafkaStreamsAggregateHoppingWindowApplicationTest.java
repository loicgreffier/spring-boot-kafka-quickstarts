package io.github.loicgreffier.streams.aggregate.hopping.window;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constant.StateStore.PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constant.Topic.PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.hopping.window.app.KafkaStreamsTopology;
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

class KafkaStreamsAggregateHoppingWindowApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsAggregateHoppingWindowApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPersonGroup> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-aggregate-hopping-window-test");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties,
            Instant.parse("2000-01-01T01:00:00Z"));

        // Create Serde for input and output topics
        Serde<KafkaPerson> personSerde = new SpecificAvroSerde<>();
        Serde<KafkaPersonGroup> personGroupSerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        personGroupSerde.configure(config, false);

        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(
            PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC,
            new StringDeserializer(),
            personGroupSerde.deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldAggregateWhenTimeWindowIsRespected() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("Homer"), Instant.parse("2000-01-01T01:00:00Z")));
        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Marge"), Instant.parse("2000-01-01T01:02:00Z")));
        inputTopic.pipeInput(new TestRecord<>("3", buildKafkaPerson("Bart"), Instant.parse("2000-01-01T01:04:00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        // Homer arrives
        assertEquals("Simpson@2000-01-01T00:56:00Z->2000-01-01T01:01:00Z", results.get(0).key);
        assertIterableEquals(List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z", results.get(1).key);
        assertIterableEquals(List.of("Homer"), results.get(1).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(2).key);
        assertIterableEquals(List.of("Homer"), results.get(2).value.getFirstNameByLastName().get("Simpson"));

        // Marge arrives
        assertEquals("Simpson@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z", results.get(3).key);
        assertIterableEquals(List.of("Homer", "Marge"), results.get(3).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(4).key);
        assertIterableEquals(List.of("Homer", "Marge"), results.get(4).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z", results.get(5).key);
        assertIterableEquals(List.of("Marge"), results.get(5).value.getFirstNameByLastName().get("Simpson"));

        // Bart arrives
        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(6).key);
        assertIterableEquals(
            List.of("Homer", "Marge", "Bart"),
            results.get(6).value.getFirstNameByLastName().get("Simpson")
        );

        assertEquals("Simpson@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z", results.get(7).key);
        assertIterableEquals(List.of("Marge", "Bart"), results.get(7).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:04:00Z->2000-01-01T01:09:00Z", results.get(8).key);
        assertIterableEquals(List.of("Bart"), results.get(8).value.getFirstNameByLastName().get("Simpson"));

        WindowStore<String, KafkaPersonGroup> stateStore = testDriver
            .getWindowStore(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue56To01 = iterator.next();
            assertEquals("Simpson", keyValue56To01.key.key());
            assertEquals("2000-01-01T00:56:00Z", keyValue56To01.key.window().startTime().toString());
            assertEquals("2000-01-01T01:01:00Z", keyValue56To01.key.window().endTime().toString());
            assertIterableEquals(List.of("Homer"), keyValue56To01.value.getFirstNameByLastName().get("Simpson"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue58To03 = iterator.next();
            assertEquals("Simpson", keyValue58To03.key.key());
            assertEquals("2000-01-01T00:58:00Z", keyValue58To03.key.window().startTime().toString());
            assertEquals("2000-01-01T01:03:00Z", keyValue58To03.key.window().endTime().toString());
            assertIterableEquals(
                List.of("Homer", "Marge"),
                keyValue58To03.value.getFirstNameByLastName().get("Simpson")
            );

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue00To05 = iterator.next();
            assertEquals("Simpson", keyValue00To05.key.key());
            assertEquals("2000-01-01T01:00:00Z", keyValue00To05.key.window().startTime().toString());
            assertEquals("2000-01-01T01:05:00Z", keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(
                List.of("Homer", "Marge", "Bart"),
                keyValue00To05.value.getFirstNameByLastName().get("Simpson")
            );

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue02To07 = iterator.next();
            assertEquals("Simpson", keyValue02To07.key.key());
            assertEquals("2000-01-01T01:02:00Z", keyValue02To07.key.window().startTime().toString());
            assertEquals("2000-01-01T01:07:00Z", keyValue02To07.key.window().endTime().toString());
            assertIterableEquals(
                List.of("Marge", "Bart"),
                keyValue02To07.value.getFirstNameByLastName().get("Simpson")
            );

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue04To09 = iterator.next();
            assertEquals("Simpson", keyValue04To09.key.key());
            assertEquals("2000-01-01T01:04:00Z", keyValue04To09.key.window().startTime().toString());
            assertEquals("2000-01-01T01:09:00Z", keyValue04To09.key.window().endTime().toString());
            assertIterableEquals(List.of("Bart"), keyValue04To09.value.getFirstNameByLastName().get("Simpson"));

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldNotAggregateWhenTimeWindowIsNotRespected() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("Homer"), Instant.parse("2000-01-01T01:00:00Z")));
        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Marge"), Instant.parse("2000-01-01T01:05:00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertEquals("Simpson@2000-01-01T00:56:00Z->2000-01-01T01:01:00Z", results.get(0).key);
        assertIterableEquals(List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z", results.get(1).key);
        assertIterableEquals(List.of("Homer"), results.get(1).value.getFirstNameByLastName().get("Simpson"));

        // The second record is not aggregated here because it is out of the time window
        // as the upper bound of hopping window is exclusive.
        // Its timestamp (01:05:00) is not included in the window [01:00:00->01:05:00).

        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(2).key);
        assertIterableEquals(List.of("Homer"), results.get(2).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z", results.get(3).key);
        assertIterableEquals(List.of("Marge"), results.get(3).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:04:00Z->2000-01-01T01:09:00Z", results.get(4).key);
        assertIterableEquals(List.of("Marge"), results.get(4).value.getFirstNameByLastName().get("Simpson"));

        WindowStore<String, KafkaPersonGroup> stateStore = testDriver
            .getWindowStore(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue56To01 = iterator.next();
            assertEquals("Simpson", keyValue56To01.key.key());
            assertEquals("2000-01-01T00:56:00Z", keyValue56To01.key.window().startTime().toString());
            assertEquals("2000-01-01T01:01:00Z", keyValue56To01.key.window().endTime().toString());
            assertIterableEquals(List.of("Homer"), keyValue56To01.value.getFirstNameByLastName().get("Simpson"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue58To03 = iterator.next();
            assertEquals("Simpson", keyValue58To03.key.key());
            assertEquals("2000-01-01T00:58:00Z", keyValue58To03.key.window().startTime().toString());
            assertEquals("2000-01-01T01:03:00Z", keyValue58To03.key.window().endTime().toString());
            assertIterableEquals(List.of("Homer"), keyValue58To03.value.getFirstNameByLastName().get("Simpson"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue00To05 = iterator.next();
            assertEquals("Simpson", keyValue00To05.key.key());
            assertEquals("2000-01-01T01:00:00Z", keyValue00To05.key.window().startTime().toString());
            assertEquals("2000-01-01T01:05:00Z", keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of("Homer"), keyValue00To05.value.getFirstNameByLastName().get("Simpson"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue02To07 = iterator.next();
            assertEquals("Simpson", keyValue02To07.key.key());
            assertEquals("2000-01-01T01:02:00Z", keyValue02To07.key.window().startTime().toString());
            assertEquals("2000-01-01T01:07:00Z", keyValue02To07.key.window().endTime().toString());
            assertIterableEquals(List.of("Marge"), keyValue02To07.value.getFirstNameByLastName().get("Simpson"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue04To09 = iterator.next();
            assertEquals("Simpson", keyValue04To09.key.key());
            assertEquals("2000-01-01T01:04:00Z", keyValue04To09.key.window().startTime().toString());
            assertEquals("2000-01-01T01:09:00Z", keyValue04To09.key.window().endTime().toString());
            assertIterableEquals(List.of("Marge"), keyValue04To09.value.getFirstNameByLastName().get("Simpson"));

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("Homer"), Instant.parse("2000-01-01T01:00:00Z")));
        inputTopic.pipeInput(new TestRecord<>("3", buildKafkaPerson("Marge"), Instant.parse("2000-01-01T01:05:30Z")));

        // At this point, the stream time is 01:05:30. It exceeds by 30 seconds
        // the upper bound of the window [01:00:00Z->01:05:00Z) where Homer is included.
        // However, the following delayed record "Bart" will be aggregated into the window
        // because the grace period is 1 minute.

        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Bart"), Instant.parse("2000-01-01T01:03:00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        // Homer arrives
        assertEquals("Simpson@2000-01-01T00:56:00Z->2000-01-01T01:01:00Z", results.get(0).key);
        assertIterableEquals(List.of("Homer"), results.get(0).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z", results.get(1).key);
        assertIterableEquals(List.of("Homer"), results.get(1).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(2).key);
        assertIterableEquals(List.of("Homer"), results.get(2).value.getFirstNameByLastName().get("Simpson"));

        // Marge arrives
        assertEquals("Simpson@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z", results.get(3).key);
        assertIterableEquals(List.of("Marge"), results.get(3).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:04:00Z->2000-01-01T01:09:00Z", results.get(4).key);
        assertIterableEquals(List.of("Marge"), results.get(4).value.getFirstNameByLastName().get("Simpson"));

        // Bart arrives
        // Even if the stream time is 01:05:30, the window [01:00:00Z->01:05:00Z) is
        // not yet closed because of the grace period of 1 minute.
        // Bart whose timestamp is 01:03:00 is included in the window.
        assertEquals("Simpson@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(5).key);
        assertIterableEquals(List.of("Homer", "Bart"), results.get(5).value.getFirstNameByLastName().get("Simpson"));

        assertEquals("Simpson@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z", results.get(6).key);
        assertIterableEquals(List.of("Marge", "Bart"), results.get(6).value.getFirstNameByLastName().get("Simpson"));

        WindowStore<String, KafkaPersonGroup> stateStore =
            testDriver.getWindowStore(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue56To01 = iterator.next();
            assertEquals("Simpson", keyValue56To01.key.key());
            assertEquals("2000-01-01T00:56:00Z", keyValue56To01.key.window().startTime().toString());
            assertEquals("2000-01-01T01:01:00Z", keyValue56To01.key.window().endTime().toString());
            assertIterableEquals(List.of("Homer"), keyValue56To01.value.getFirstNameByLastName().get("Simpson"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue58To03 = iterator.next();
            assertEquals("Simpson", keyValue58To03.key.key());
            assertEquals("2000-01-01T00:58:00Z", keyValue58To03.key.window().startTime().toString());
            assertEquals("2000-01-01T01:03:00Z", keyValue58To03.key.window().endTime().toString());
            assertIterableEquals(
                List.of("Homer"),
                keyValue58To03.value.getFirstNameByLastName().get("Simpson")
            );

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue00To05 = iterator.next();
            assertEquals("Simpson", keyValue00To05.key.key());
            assertEquals("2000-01-01T01:00:00Z", keyValue00To05.key.window().startTime().toString());
            assertEquals("2000-01-01T01:05:00Z", keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(
                List.of("Homer", "Bart"),
                keyValue00To05.value.getFirstNameByLastName().get("Simpson")
            );

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue02To07 = iterator.next();
            assertEquals("Simpson", keyValue02To07.key.key());
            assertEquals("2000-01-01T01:02:00Z", keyValue02To07.key.window().startTime().toString());
            assertEquals("2000-01-01T01:07:00Z", keyValue02To07.key.window().endTime().toString());
            assertIterableEquals(
                List.of("Marge", "Bart"),
                keyValue02To07.value.getFirstNameByLastName().get("Simpson")
            );

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue04To09 = iterator.next();
            assertEquals("Simpson", keyValue04To09.key.key());
            assertEquals("2000-01-01T01:04:00Z", keyValue04To09.key.window().startTime().toString());
            assertEquals("2000-01-01T01:09:00Z", keyValue04To09.key.window().endTime().toString());
            assertIterableEquals(List.of("Marge"), keyValue04To09.value.getFirstNameByLastName().get("Simpson"));

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
