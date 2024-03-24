package io.github.loicgreffier.streams.aggregate.tumbling.window;

import static io.github.loicgreffier.streams.aggregate.tumbling.window.constant.StateStore.PERSON_AGGREGATE_TUMBLING_WINDOW_STATE_STORE;
import static io.github.loicgreffier.streams.aggregate.tumbling.window.constant.Topic.PERSON_AGGREGATE_TUMBLING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.tumbling.window.constant.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.tumbling.window.app.KafkaStreamsTopology;
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
 * This class contains unit tests for the {@link KafkaStreamsTopology} class.
 */
class KafkaStreamsAggregateTumblingWindowApplicationTests {
    private static final String SCHEMA_REGISTRY_SCOPE =
        KafkaStreamsAggregateTumblingWindowApplicationTests.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPersonGroup> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,
            "streams-aggregate-tumbling-window-test");
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
        Serde<KafkaPersonGroup> personGroupSerde = new SpecificAvroSerde<>();
        Map<String, String> config =
            Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        personGroupSerde.configure(config, false);

        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(),
            personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(PERSON_AGGREGATE_TUMBLING_WINDOW_TOPIC,
            new StringDeserializer(), personGroupSerde.deserializer());
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
            Instant.parse("2000-01-01T01:00:00.00Z")));
        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Jane", "Smith"),
            Instant.parse("2000-01-01T01:01:00.00Z")));
        inputTopic.pipeInput(new TestRecord<>("3", buildKafkaPerson("Michael", "Doe"),
            Instant.parse("2000-01-01T01:02:00.00Z")));
        inputTopic.pipeInput(new TestRecord<>("4", buildKafkaPerson("Daniel", "Smith"),
            Instant.parse("2000-01-01T01:04:00.00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Doe@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Doe")).containsExactly(
            "John");

        assertThat(results.get(1).key).isEqualTo(
            "Smith@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Smith")).containsExactly(
            "Jane");

        assertThat(results.get(2).key).isEqualTo("Doe@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Doe")).containsExactly(
            "John", "Michael");

        assertThat(results.get(3).key).isEqualTo(
            "Smith@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Smith")).containsExactly(
            "Jane", "Daniel");

        // Check state store
        WindowStore<String, KafkaPersonGroup> stateStore =
            testDriver.getWindowStore(PERSON_AGGREGATE_TUMBLING_WINDOW_STATE_STORE);
        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueDoe00To05 = iterator.next();
            assertThat(keyValueDoe00To05.key.key()).isEqualTo("Doe");
            assertThat(keyValueDoe00To05.key.window().startTime()).isEqualTo(
                "2000-01-01T01:00:00.00Z");
            assertThat(keyValueDoe00To05.key.window().endTime()).isEqualTo(
                "2000-01-01T01:05:00.00Z");
            assertThat(
                keyValueDoe00To05.value.getFirstNameByLastName().get("Doe")).containsExactly(
                "John", "Michael");

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValueSmith00To05 = iterator.next();
            assertThat(keyValueSmith00To05.key.key()).isEqualTo("Smith");
            assertThat(keyValueSmith00To05.key.window().startTime()).isEqualTo(
                "2000-01-01T01:00:00.00Z");
            assertThat(keyValueSmith00To05.key.window().endTime()).isEqualTo(
                "2000-01-01T01:05:00.00Z");
            assertThat(
                keyValueSmith00To05.value.getFirstNameByLastName()
                    .get("Smith")).containsExactly(
                "Jane", "Daniel");

            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void shouldNotAggregateWhenTimeWindowIsNotRespected() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("John", "Doe"),
            Instant.parse("2000-01-01T01:00:00.00Z")));
        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Michael", "Doe"),
            Instant.parse("2000-01-01T01:05:00.00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        // The second record is not aggregated here because it is out of the time window
        // as the upper bound of tumbling window is exclusive.
        // Its timestamp (01:05:00) is not included in the window (01:00:00->01:05:00)
        assertThat(results.get(0).key).isEqualTo("Doe@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Doe")).containsExactly(
            "John");

        assertThat(results.get(1).key).isEqualTo("Doe@2000-01-01T01:05:00Z->2000-01-01T01:10:00Z");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Doe")).containsExactly(
            "Michael");

        // Check state store
        WindowStore<String, KafkaPersonGroup> stateStore =
            testDriver.getWindowStore(PERSON_AGGREGATE_TUMBLING_WINDOW_STATE_STORE);
        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue00To05 = iterator.next();
            assertThat(keyValue00To05.key.key()).isEqualTo("Doe");
            assertThat(keyValue00To05.key.window().startTime()).isEqualTo(
                "2000-01-01T01:00:00.00Z");
            assertThat(keyValue00To05.key.window().endTime()).isEqualTo(
                "2000-01-01T01:05:00.00Z");
            assertThat(
                keyValue00To05.value.getFirstNameByLastName().get("Doe")).containsExactly(
                "John");

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue05To10 = iterator.next();
            assertThat(keyValue05To10.key.key()).isEqualTo("Doe");
            assertThat(keyValue05To10.key.window().startTime()).isEqualTo(
                "2000-01-01T01:05:00.00Z");
            assertThat(keyValue05To10.key.window().endTime()).isEqualTo(
                "2000-01-01T01:10:00.00Z");
            assertThat(
                keyValue05To10.value.getFirstNameByLastName().get("Doe")).containsExactly(
                "Michael");

            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("John", "Doe"),
            Instant.parse("2000-01-01T01:00:00.00Z")));
        inputTopic.pipeInput(new TestRecord<>("3", buildKafkaPerson("Michael", "Doe"),
            Instant.parse("2000-01-01T01:05:30.00Z")));

        // At this point, the stream time is 01:05:30. It exceeds the upper bound of the
        // window (01:00:00.00Z->01:05:00.00Z) where John is included by 30 seconds.
        // However, the following delayed record will be aggregated into the window
        // because the grace period is 1 minute.

        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Gio", "Doe"),
            Instant.parse("2000-01-01T01:03:00.00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Doe@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Doe")).containsExactly(
            "John");

        assertThat(results.get(1).key).isEqualTo("Doe@2000-01-01T01:05:00Z->2000-01-01T01:10:00Z");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Doe")).containsExactly(
            "Michael");

        // Gio is aggregated here because the grace period is 1 minute
        assertThat(results.get(2).key).isEqualTo("Doe@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Doe")).containsExactly(
            "John", "Gio");

        // Check state store
        WindowStore<String, KafkaPersonGroup> stateStore =
            testDriver.getWindowStore(PERSON_AGGREGATE_TUMBLING_WINDOW_STATE_STORE);
        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue00To05 = iterator.next();
            assertThat(keyValue00To05.key.key()).isEqualTo("Doe");
            assertThat(keyValue00To05.key.window().startTime()).isEqualTo(
                "2000-01-01T01:00:00Z");
            assertThat(keyValue00To05.key.window().endTime()).isEqualTo(
                "2000-01-01T01:05:00.00Z");
            assertThat(
                keyValue00To05.value.getFirstNameByLastName().get("Doe")).containsExactly(
                "John", "Gio");

            KeyValue<Windowed<String>, KafkaPersonGroup> keyValue05To10 = iterator.next();
            assertThat(keyValue05To10.key.key()).isEqualTo("Doe");
            assertThat(keyValue05To10.key.window().startTime()).isEqualTo(
                "2000-01-01T01:05:00Z");
            assertThat(keyValue05To10.key.window().endTime()).isEqualTo(
                "2000-01-01T01:10:00.00Z");
            assertThat(
                keyValue05To10.value.getFirstNameByLastName().get("Doe")).containsExactly(
                "Michael");

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
