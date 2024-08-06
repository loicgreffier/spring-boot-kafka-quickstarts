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

/**
 * This class contains unit tests for the {@link KafkaStreamsTopology} class.
 */
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
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("John", "Doe"),
            Instant.parse("2000-01-01T01:00:00Z")));
        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Jane", "Smith"),
            Instant.parse("2000-01-01T01:01:00Z")));
        inputTopic.pipeInput(new TestRecord<>("3", buildKafkaPerson("Michael", "Doe"),
            Instant.parse("2000-01-01T01:02:00Z")));
        inputTopic.pipeInput(new TestRecord<>("4", buildKafkaPerson("Daniel", "Smith"),
            Instant.parse("2000-01-01T01:04:00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertEquals("Doe@2000-01-01T00:56:00Z->2000-01-01T01:01:00Z", results.get(0).key);
        assertIterableEquals(List.of("John"), results.get(0).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z", results.get(1).key);
        assertIterableEquals(List.of("John"), results.get(1).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(2).key);
        assertIterableEquals(List.of("John"), results.get(2).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Smith@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z", results.get(3).key);
        assertIterableEquals(List.of("Jane"), results.get(3).value.getFirstNameByLastName().get("Smith"));

        assertEquals("Smith@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(4).key);
        assertIterableEquals(List.of("Jane"), results.get(4).value.getFirstNameByLastName().get("Smith"));

        assertEquals("Doe@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z", results.get(5).key);
        assertIterableEquals(List.of("John", "Michael"), results.get(5).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(6).key);
        assertIterableEquals(List.of("John", "Michael"), results.get(6).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z", results.get(7).key);
        assertIterableEquals(List.of("Michael"), results.get(7).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Smith@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(8).key);
        assertIterableEquals(List.of("Jane", "Daniel"), results.get(8).value.getFirstNameByLastName().get("Smith"));

        assertEquals("Smith@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z", results.get(9).key);
        assertIterableEquals(List.of("Daniel"), results.get(9).value.getFirstNameByLastName().get("Smith"));

        assertEquals("Smith@2000-01-01T01:04:00Z->2000-01-01T01:09:00Z", results.get(10).key);
        assertIterableEquals(List.of("Daniel"), results.get(10).value.getFirstNameByLastName().get("Smith"));

        // Check state store
        WindowStore<String, KafkaPersonGroup> stateStore =
            testDriver.getWindowStore(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE);
        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueDoe56To01 = iterator.next();
            assertEquals("Doe", keyValueDoe56To01.key.key());
            assertEquals("2000-01-01T00:56:00Z", keyValueDoe56To01.key.window().startTime().toString());
            assertEquals("2000-01-01T01:01:00Z", keyValueDoe56To01.key.window().endTime().toString());
            assertIterableEquals(List.of("John"), keyValueDoe56To01.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueDoe58To03 = iterator.next();
            assertEquals("Doe", keyValueDoe58To03.key.key());
            assertEquals("2000-01-01T00:58:00Z", keyValueDoe58To03.key.window().startTime().toString());
            assertEquals("2000-01-01T01:03:00Z", keyValueDoe58To03.key.window().endTime().toString());
            assertIterableEquals(List.of("John", "Michael"),
                keyValueDoe58To03.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueSmith58To03 = iterator.next();
            assertEquals("Smith", keyValueSmith58To03.key.key());
            assertEquals("2000-01-01T00:58:00Z", keyValueSmith58To03.key.window().startTime().toString());
            assertEquals("2000-01-01T01:03:00Z", keyValueSmith58To03.key.window().endTime().toString());
            assertIterableEquals(List.of("Jane"), keyValueSmith58To03.value.getFirstNameByLastName().get("Smith"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueDoe00To05 = iterator.next();
            assertEquals("Doe", keyValueDoe00To05.key.key());
            assertEquals("2000-01-01T01:00:00Z", keyValueDoe00To05.key.window().startTime().toString());
            assertEquals("2000-01-01T01:05:00Z", keyValueDoe00To05.key.window().endTime().toString());
            assertIterableEquals(List.of("John", "Michael"),
                keyValueDoe00To05.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueDoe02To07 = iterator.next();
            assertEquals("Doe", keyValueDoe02To07.key.key());
            assertEquals("2000-01-01T01:02:00Z", keyValueDoe02To07.key.window().startTime().toString());
            assertEquals("2000-01-01T01:07:00Z", keyValueDoe02To07.key.window().endTime().toString());
            assertIterableEquals(List.of("Michael"), keyValueDoe02To07.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueSmith00To05 = iterator.next();
            assertEquals("Smith", keyValueSmith00To05.key.key());
            assertEquals("2000-01-01T01:00:00Z",  keyValueSmith00To05.key.window().startTime().toString());
            assertEquals("2000-01-01T01:05:00Z", keyValueSmith00To05.key.window().endTime().toString());
            assertIterableEquals(List.of("Jane", "Daniel"),
                keyValueSmith00To05.value.getFirstNameByLastName().get("Smith"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueSmith02To07 = iterator.next();
            assertEquals("Smith", keyValueSmith02To07.key.key());
            assertEquals("2000-01-01T01:02:00Z", keyValueSmith02To07.key.window().startTime().toString());
            assertEquals("2000-01-01T01:07:00Z", keyValueSmith02To07.key.window().endTime().toString());
            assertIterableEquals(List.of("Daniel"), keyValueSmith02To07.value.getFirstNameByLastName().get("Smith"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueSmith04To09 = iterator.next();
            assertEquals("Smith", keyValueSmith04To09.key.key());
            assertEquals("2000-01-01T01:04:00Z", keyValueSmith04To09.key.window().startTime().toString());
            assertEquals("2000-01-01T01:09:00Z", keyValueSmith04To09.key.window().endTime().toString());
            assertIterableEquals(List.of("Daniel"), keyValueSmith04To09.value.getFirstNameByLastName().get("Smith"));

            assertFalse(iterator.hasNext());
        }
    }

    @Test
    void shouldNotAggregateWhenTimeWindowIsNotRespected() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("John", "Doe"),
            Instant.parse("2000-01-01T01:00:00Z")));
        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Michael", "Doe"),
            Instant.parse("2000-01-01T01:05:00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertEquals("Doe@2000-01-01T00:56:00Z->2000-01-01T01:01:00Z", results.get(0).key);
        assertIterableEquals(List.of("John"), results.get(0).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z", results.get(1).key);
        assertIterableEquals(List.of("John"), results.get(1).value.getFirstNameByLastName().get("Doe"));

        // The second record is not aggregated here because it is out of the time window
        // as the upper bound of hopping window is exclusive.
        // Its timestamp (01:05:00) is not included in the window [01:00:00->01:05:00)

        assertEquals("Doe@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(2).key);
        assertIterableEquals(List.of("John"), results.get(2).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z", results.get(3).key);
        assertIterableEquals(List.of("Michael"), results.get(3).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T01:04:00Z->2000-01-01T01:09:00Z", results.get(4).key);
        assertIterableEquals(List.of("Michael"), results.get(4).value.getFirstNameByLastName().get("Doe"));

        // Check state store
        WindowStore<String, KafkaPersonGroup> stateStore =
            testDriver.getWindowStore(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE);
        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue56To01 = iterator.next();
            assertEquals("Doe", keyValue56To01.key.key());
            assertEquals("2000-01-01T00:56:00Z", keyValue56To01.key.window().startTime().toString());
            assertEquals("2000-01-01T01:01:00Z", keyValue56To01.key.window().endTime().toString());
            assertIterableEquals(List.of("John"), keyValue56To01.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue58To03 = iterator.next();
            assertEquals("Doe", keyValue58To03.key.key());
            assertEquals("2000-01-01T00:58:00Z", keyValue58To03.key.window().startTime().toString());
            assertEquals("2000-01-01T01:03:00Z", keyValue58To03.key.window().endTime().toString());
            assertIterableEquals(List.of("John"), keyValue58To03.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue00To05 = iterator.next();
            assertEquals("Doe", keyValue00To05.key.key());
            assertEquals("2000-01-01T01:00:00Z", keyValue00To05.key.window().startTime().toString());
            assertEquals("2000-01-01T01:05:00Z", keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of("John"), keyValue00To05.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue02To07 = iterator.next();
            assertEquals("Doe", keyValue02To07.key.key());
            assertEquals("2000-01-01T01:02:00Z", keyValue02To07.key.window().startTime().toString());
            assertEquals("2000-01-01T01:07:00Z", keyValue02To07.key.window().endTime().toString());
            assertIterableEquals(List.of("Michael"), keyValue02To07.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue04To09 = iterator.next();
            assertEquals("Doe", keyValue04To09.key.key());
            assertEquals("2000-01-01T01:04:00Z", keyValue04To09.key.window().startTime().toString());
            assertEquals("2000-01-01T01:09:00Z", keyValue04To09.key.window().endTime().toString());
            assertIterableEquals(List.of("Michael"), keyValue04To09.value.getFirstNameByLastName().get("Doe"));

            assertFalse(iterator.hasNext());
        }
    }

    /**
     * This test case demonstrates how the grace period works.
     * Grace period defines how long to wait for out-of-order records.
     * Windows with grace period will continue to accept late records
     * until stream time >= window end time + grace period.
     */
    @Test
    void shouldHonorGracePeriod() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("John", "Doe"),
            Instant.parse("2000-01-01T01:00:00Z")));
        inputTopic.pipeInput(new TestRecord<>("3", buildKafkaPerson("Michael", "Doe"),
            Instant.parse("2000-01-01T01:05:30Z")));

        // At this point, the stream time is 01:05:30. It exceeds by 30 seconds
        // the upper bound of the window [01:00:00Z->01:05:00Z) where John is included.
        // However, the following delayed record "Gio" will be aggregated into the window
        // because the grace period is 1 minute.

        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Gio", "Doe"),
            Instant.parse("2000-01-01T01:03:00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertEquals("Doe@2000-01-01T00:56:00Z->2000-01-01T01:01:00Z", results.get(0).key);
        assertIterableEquals(List.of("John"), results.get(0).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z", results.get(1).key);
        assertIterableEquals(List.of("John"), results.get(1).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(2).key);
        assertIterableEquals(List.of("John"), results.get(2).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z", results.get(3).key);
        assertIterableEquals(List.of("Michael"), results.get(3).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T01:04:00Z->2000-01-01T01:09:00Z", results.get(4).key);
        assertIterableEquals(List.of("Michael"), results.get(4).value.getFirstNameByLastName().get("Doe"));

        // Even if the stream time is 01:05:30 and the window [01:00:00Z->01:05:00Z) is supposed to be closed,
        // Gio whose timestamp is 01:03:00 is included in the window.
        // Stream time < window end time + grace period is true, so the record is aggregated.

        assertEquals("Doe@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z", results.get(5).key);
        assertIterableEquals(List.of("John", "Gio"), results.get(5).value.getFirstNameByLastName().get("Doe"));

        assertEquals("Doe@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z", results.get(6).key);
        assertIterableEquals(List.of("Michael", "Gio"), results.get(6).value.getFirstNameByLastName().get("Doe"));

        // Check state store
        WindowStore<String, KafkaPersonGroup> stateStore =
            testDriver.getWindowStore(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE);
        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue56To01 = iterator.next();
            assertEquals("Doe", keyValue56To01.key.key());
            assertEquals("2000-01-01T00:56:00Z", keyValue56To01.key.window().startTime().toString());
            assertEquals("2000-01-01T01:01:00Z", keyValue56To01.key.window().endTime().toString());
            assertIterableEquals(List.of("John"), keyValue56To01.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue58To03 = iterator.next();
            assertEquals("Doe", keyValue58To03.key.key());
            assertEquals("2000-01-01T00:58:00Z", keyValue58To03.key.window().startTime().toString());
            assertEquals("2000-01-01T01:03:00Z", keyValue58To03.key.window().endTime().toString());
            assertIterableEquals(List.of("John"), keyValue58To03.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue00To05 = iterator.next();
            assertEquals("Doe", keyValue00To05.key.key());
            assertEquals("2000-01-01T01:00:00Z", keyValue00To05.key.window().startTime().toString());
            assertEquals("2000-01-01T01:05:00Z", keyValue00To05.key.window().endTime().toString());
            assertIterableEquals(List.of("John", "Gio"), keyValue00To05.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue02To07 = iterator.next();
            assertEquals("Doe", keyValue02To07.key.key());
            assertEquals("2000-01-01T01:02:00Z", keyValue02To07.key.window().startTime().toString());
            assertEquals("2000-01-01T01:07:00Z", keyValue02To07.key.window().endTime().toString());
            assertIterableEquals(List.of("Michael", "Gio"), keyValue02To07.value.getFirstNameByLastName().get("Doe"));

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue04To09 = iterator.next();
            assertEquals("Doe", keyValue04To09.key.key());
            assertEquals("2000-01-01T01:04:00Z", keyValue04To09.key.window().startTime().toString());
            assertEquals("2000-01-01T01:09:00Z", keyValue04To09.key.window().endTime().toString());
            assertIterableEquals(List.of("Michael"), keyValue04To09.value.getFirstNameByLastName().get("Doe"));

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
