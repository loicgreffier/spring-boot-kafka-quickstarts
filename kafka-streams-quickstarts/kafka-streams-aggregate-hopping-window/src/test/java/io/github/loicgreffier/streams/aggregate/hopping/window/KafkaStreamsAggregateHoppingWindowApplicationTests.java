package io.github.loicgreffier.streams.aggregate.hopping.window;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.hopping.window.app.KafkaStreamsTopology;
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

class KafkaStreamsAggregateHoppingWindowApplicationTests {
    private static final String SCHEMA_REGISTRY_SCOPE = KafkaStreamsAggregateHoppingWindowApplicationTests.class.getName();
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
        KafkaStreamsTopology.topology(streamsBuilder);
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
    void shouldAggregateWhenTimeWindowIsRespected() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("Aaran", "Allen"), Instant.parse("2000-01-01T01:00:00.00Z")));
        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Bret", "Wise"), Instant.parse("2000-01-01T01:01:00.00Z")));
        inputTopic.pipeInput(new TestRecord<>("3", buildKafkaPerson("Brendan", "Allen"), Instant.parse("2000-01-01T01:02:00.00Z")));
        inputTopic.pipeInput(new TestRecord<>("4", buildKafkaPerson("Jude", "Wise"), Instant.parse("2000-01-01T01:04:00.00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Allen@2000-01-01T00:56:00Z->2000-01-01T01:01:00Z");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

        assertThat(results.get(1).key).isEqualTo("Allen@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

        assertThat(results.get(2).key).isEqualTo("Allen@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

        assertThat(results.get(3).key).isEqualTo("Wise@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Wise")).containsExactly("Bret");

        assertThat(results.get(4).key).isEqualTo("Wise@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(4).value.getFirstNameByLastName().get("Wise")).containsExactly("Bret");

        assertThat(results.get(5).key).isEqualTo("Allen@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z");
        assertThat(results.get(5).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran", "Brendan");

        assertThat(results.get(6).key).isEqualTo("Allen@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(6).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran", "Brendan");

        assertThat(results.get(7).key).isEqualTo("Allen@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z");
        assertThat(results.get(7).value.getFirstNameByLastName().get("Allen")).containsExactly("Brendan");

        assertThat(results.get(8).key).isEqualTo("Wise@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(8).value.getFirstNameByLastName().get("Wise")).containsExactly("Bret", "Jude");

        assertThat(results.get(9).key).isEqualTo("Wise@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z");
        assertThat(results.get(9).value.getFirstNameByLastName().get("Wise")).containsExactly("Jude");

        assertThat(results.get(10).key).isEqualTo("Wise@2000-01-01T01:04:00Z->2000-01-01T01:09:00Z");
        assertThat(results.get(10).value.getFirstNameByLastName().get("Wise")).containsExactly("Jude");

        // Explore state store
        WindowStore<String, KafkaPersonGroup> stateStore = testDriver.getWindowStore(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE);
        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen00h56_01h01 = iterator.next();
            assertThat(windowAllen00h56_01h01.key.key()).isEqualTo("Allen");
            assertThat(windowAllen00h56_01h01.key.window().startTime()).isEqualTo("2000-01-01T00:56:00Z");
            assertThat(windowAllen00h56_01h01.key.window().endTime()).isEqualTo("2000-01-01T01:01:00.00Z");
            assertThat(windowAllen00h56_01h01.value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen00h58_01h03 = iterator.next();
            assertThat(windowAllen00h58_01h03.key.key()).isEqualTo("Allen");
            assertThat(windowAllen00h58_01h03.key.window().startTime()).isEqualTo("2000-01-01T00:58:00Z");
            assertThat(windowAllen00h58_01h03.key.window().endTime()).isEqualTo("2000-01-01T01:03:00.00Z");
            assertThat(windowAllen00h58_01h03.value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran", "Brendan");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowWise00h58_01h03 = iterator.next();
            assertThat(windowWise00h58_01h03.key.key()).isEqualTo("Wise");
            assertThat(windowWise00h58_01h03.key.window().startTime()).isEqualTo("2000-01-01T00:58:00Z");
            assertThat(windowWise00h58_01h03.key.window().endTime()).isEqualTo("2000-01-01T01:03:00.00Z");
            assertThat(windowWise00h58_01h03.value.getFirstNameByLastName().get("Wise")).containsExactly("Bret");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen01h00_01h05 = iterator.next();
            assertThat(windowAllen01h00_01h05.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h00_01h05.key.window().startTime()).isEqualTo("2000-01-01T01:00:00Z");
            assertThat(windowAllen01h00_01h05.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(windowAllen01h00_01h05.value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran", "Brendan");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen01h02_01h07 = iterator.next();
            assertThat(windowAllen01h02_01h07.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h02_01h07.key.window().startTime()).isEqualTo("2000-01-01T01:02:00Z");
            assertThat(windowAllen01h02_01h07.key.window().endTime()).isEqualTo("2000-01-01T01:07:00.00Z");
            assertThat(windowAllen01h02_01h07.value.getFirstNameByLastName().get("Allen")).containsExactly("Brendan");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowWise01h00_01h05 = iterator.next();
            assertThat(windowWise01h00_01h05.key.key()).isEqualTo("Wise");
            assertThat(windowWise01h00_01h05.key.window().startTime()).isEqualTo("2000-01-01T01:00:00Z");
            assertThat(windowWise01h00_01h05.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(windowWise01h00_01h05.value.getFirstNameByLastName().get("Wise")).containsExactly("Bret", "Jude");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowWise01h02_01h07 = iterator.next();
            assertThat(windowWise01h02_01h07.key.key()).isEqualTo("Wise");
            assertThat(windowWise01h02_01h07.key.window().startTime()).isEqualTo("2000-01-01T01:02:00Z");
            assertThat(windowWise01h02_01h07.key.window().endTime()).isEqualTo("2000-01-01T01:07:00.00Z");
            assertThat(windowWise01h02_01h07.value.getFirstNameByLastName().get("Wise")).containsExactly("Jude");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowWise01h04_01h09 = iterator.next();
            assertThat(windowWise01h04_01h09.key.key()).isEqualTo("Wise");
            assertThat(windowWise01h04_01h09.key.window().startTime()).isEqualTo("2000-01-01T01:04:00Z");
            assertThat(windowWise01h04_01h09.key.window().endTime()).isEqualTo("2000-01-01T01:09:00.00Z");
            assertThat(windowWise01h04_01h09.value.getFirstNameByLastName().get("Wise")).containsExactly("Jude");

            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void shouldNotAggregateWhenTimeWindowIsNotRespected() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("Aaran", "Allen"), Instant.parse("2000-01-01T01:00:00.00Z")));
        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Brendan", "Allen"), Instant.parse("2000-01-01T01:05:00.00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Allen@2000-01-01T00:56:00Z->2000-01-01T01:01:00Z");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

        assertThat(results.get(1).key).isEqualTo("Allen@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

        // Upper bound of hopping windows is exclusive, so Brendan is not in it
        assertThat(results.get(2).key).isEqualTo("Allen@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

        assertThat(results.get(3).key).isEqualTo("Allen@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Allen")).containsExactly("Brendan");

        assertThat(results.get(4).key).isEqualTo("Allen@2000-01-01T01:04:00Z->2000-01-01T01:09:00Z");
        assertThat(results.get(4).value.getFirstNameByLastName().get("Allen")).containsExactly("Brendan");

        // Explore state store
        WindowStore<String, KafkaPersonGroup> stateStore = testDriver.getWindowStore(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE);
        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen00h56_01h01 = iterator.next();
            assertThat(windowAllen00h56_01h01.key.key()).isEqualTo("Allen");
            assertThat(windowAllen00h56_01h01.key.window().startTime()).isEqualTo("2000-01-01T00:56:00Z");
            assertThat(windowAllen00h56_01h01.key.window().endTime()).isEqualTo("2000-01-01T01:01:00.00Z");
            assertThat(windowAllen00h56_01h01.value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen00h58_01h03 = iterator.next();
            assertThat(windowAllen00h58_01h03.key.key()).isEqualTo("Allen");
            assertThat(windowAllen00h58_01h03.key.window().startTime()).isEqualTo("2000-01-01T00:58:00Z");
            assertThat(windowAllen00h58_01h03.key.window().endTime()).isEqualTo("2000-01-01T01:03:00.00Z");
            assertThat(windowAllen00h58_01h03.value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen01h00_01h05 = iterator.next();
            assertThat(windowAllen01h00_01h05.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h00_01h05.key.window().startTime()).isEqualTo("2000-01-01T01:00:00Z");
            assertThat(windowAllen01h00_01h05.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(windowAllen01h00_01h05.value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen01h02_01h07 = iterator.next();
            assertThat(windowAllen01h02_01h07.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h02_01h07.key.window().startTime()).isEqualTo("2000-01-01T01:02:00Z");
            assertThat(windowAllen01h02_01h07.key.window().endTime()).isEqualTo("2000-01-01T01:07:00.00Z");
            assertThat(windowAllen01h02_01h07.value.getFirstNameByLastName().get("Allen")).containsExactly("Brendan");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen01h04_01h09 = iterator.next();
            assertThat(windowAllen01h04_01h09.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h04_01h09.key.window().startTime()).isEqualTo("2000-01-01T01:04:00Z");
            assertThat(windowAllen01h04_01h09.key.window().endTime()).isEqualTo("2000-01-01T01:09:00.00Z");
            assertThat(windowAllen01h04_01h09.value.getFirstNameByLastName().get("Allen")).containsExactly("Brendan");

            assertThat(iterator.hasNext()).isFalse();
        }
    }

    @Test
    void shouldHonorGracePeriod() {
        inputTopic.pipeInput(new TestRecord<>("1", buildKafkaPerson("Aaran", "Allen"), Instant.parse("2000-01-01T01:00:00.00Z")));
        inputTopic.pipeInput(new TestRecord<>("3", buildKafkaPerson("Brendan", "Allen"), Instant.parse("2000-01-01T01:05:30.00Z")));

        // At this point, the stream time (01:05:30) is after the end of the first record last window (Allen@01:00:00->01:05:00)
        // But, this delayed record will match the first record last window thanks to grace period that keeps window opened for 1 more minute
        inputTopic.pipeInput(new TestRecord<>("2", buildKafkaPerson("Gio", "Allen"), Instant.parse("2000-01-01T01:03:00.00Z")));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Allen@2000-01-01T00:56:00Z->2000-01-01T01:01:00Z");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

        assertThat(results.get(1).key).isEqualTo("Allen@2000-01-01T00:58:00Z->2000-01-01T01:03:00Z");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

        assertThat(results.get(2).key).isEqualTo("Allen@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

        assertThat(results.get(3).key).isEqualTo("Allen@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Allen")).containsExactly("Brendan");

        assertThat(results.get(4).key).isEqualTo("Allen@2000-01-01T01:04:00Z->2000-01-01T01:09:00Z");
        assertThat(results.get(4).value.getFirstNameByLastName().get("Allen")).containsExactly("Brendan");

        assertThat(results.get(5).key).isEqualTo("Allen@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(5).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran", "Gio");

        assertThat(results.get(6).key).isEqualTo("Allen@2000-01-01T01:02:00Z->2000-01-01T01:07:00Z");
        assertThat(results.get(6).value.getFirstNameByLastName().get("Allen")).containsExactly("Brendan", "Gio");

        // Explore state store
        WindowStore<String, KafkaPersonGroup> stateStore = testDriver.getWindowStore(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE);
        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen00h56_01h01 = iterator.next();
            assertThat(windowAllen00h56_01h01.key.key()).isEqualTo("Allen");
            assertThat(windowAllen00h56_01h01.key.window().startTime()).isEqualTo("2000-01-01T00:56:00Z");
            assertThat(windowAllen00h56_01h01.key.window().endTime()).isEqualTo("2000-01-01T01:01:00.00Z");
            assertThat(windowAllen00h56_01h01.value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen00h58_01h03 = iterator.next();
            assertThat(windowAllen00h58_01h03.key.key()).isEqualTo("Allen");
            assertThat(windowAllen00h58_01h03.key.window().startTime()).isEqualTo("2000-01-01T00:58:00Z");
            assertThat(windowAllen00h58_01h03.key.window().endTime()).isEqualTo("2000-01-01T01:03:00.00Z");
            assertThat(windowAllen00h58_01h03.value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen01h00_01h05 = iterator.next();
            assertThat(windowAllen01h00_01h05.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h00_01h05.key.window().startTime()).isEqualTo("2000-01-01T01:00:00Z");
            assertThat(windowAllen01h00_01h05.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(windowAllen01h00_01h05.value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran", "Gio");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen01h02_01h07 = iterator.next();
            assertThat(windowAllen01h02_01h07.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h02_01h07.key.window().startTime()).isEqualTo("2000-01-01T01:02:00Z");
            assertThat(windowAllen01h02_01h07.key.window().endTime()).isEqualTo("2000-01-01T01:07:00.00Z");
            assertThat(windowAllen01h02_01h07.value.getFirstNameByLastName().get("Allen")).containsExactly("Brendan", "Gio");

            KeyValue<Windowed<String>, KafkaPersonGroup> windowAllen01h04_01h09 = iterator.next();
            assertThat(windowAllen01h04_01h09.key.key()).isEqualTo("Allen");
            assertThat(windowAllen01h04_01h09.key.window().startTime()).isEqualTo("2000-01-01T01:04:00Z");
            assertThat(windowAllen01h04_01h09.key.window().endTime()).isEqualTo("2000-01-01T01:09:00.00Z");
            assertThat(windowAllen01h04_01h09.value.getFirstNameByLastName().get("Allen")).containsExactly("Brendan");

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
