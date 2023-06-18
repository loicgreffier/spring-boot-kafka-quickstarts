package io.github.loicgreffier.streams.aggregate.tumbling.window;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.tumbling.window.app.KafkaStreamsAggregateTumblingWindowTopology;
import io.github.loicgreffier.streams.aggregate.tumbling.window.constants.StateStore;
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

import static io.github.loicgreffier.streams.aggregate.tumbling.window.constants.StateStore.PERSON_AGGREGATE_TUMBLING_WINDOW_STATE_STORE;
import static io.github.loicgreffier.streams.aggregate.tumbling.window.constants.Topic.PERSON_AGGREGATE_TUMBLING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.tumbling.window.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsAggregateTumblingWindowTest {
    private static final String SCHEMA_REGISTRY_SCOPE = KafkaStreamsAggregateTumblingWindowTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPersonGroup> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-aggregate-tumbling-window-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsAggregateTumblingWindowTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        // Create Serde for input and output topics
        Serde<KafkaPerson> personSerde = new SpecificAvroSerde<>();
        Serde<KafkaPersonGroup> personGroupSerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        personGroupSerde.configure(config, false);

        inputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(PERSON_AGGREGATE_TUMBLING_WINDOW_TOPIC, new StringDeserializer(), personGroupSerde.deserializer());
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

        assertThat(results).hasSize(6);
        assertThat(results.get(0).key).isEqualTo("Abbott@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

        assertThat(results.get(1).key).isEqualTo("Abbott@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan");

        assertThat(results.get(2).key).isEqualTo("Holman@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Holman")).containsExactly("Bret");

        assertThat(results.get(3).key).isEqualTo("Abbott@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan", "Daimhin");

        assertThat(results.get(4).key).isEqualTo("Patton@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(4).value.getFirstNameByLastName().get("Patton")).containsExactly("Jiao");

        assertThat(results.get(5).key).isEqualTo("Holman@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(5).value.getFirstNameByLastName().get("Holman")).containsExactly("Bret", "Jude");

        WindowStore<String, KafkaPersonGroup> stateStore = testDriver.getWindowStore(PERSON_AGGREGATE_TUMBLING_WINDOW_STATE_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> abbottAggregation = iterator.next();
            assertThat(abbottAggregation.key.key()).isEqualTo("Abbott");
            assertThat(abbottAggregation.key.window().startTime()).isEqualTo("2000-01-01T01:00:00.00Z");
            assertThat(abbottAggregation.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(abbottAggregation.value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan", "Daimhin");

            KeyValue<Windowed<String>, KafkaPersonGroup> holmanAggregation = iterator.next();
            assertThat(holmanAggregation.key.key()).isEqualTo("Holman");
            assertThat(holmanAggregation.key.window().startTime()).isEqualTo("2000-01-01T01:00:00.00Z");
            assertThat(holmanAggregation.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(holmanAggregation.value.getFirstNameByLastName().get("Holman")).containsExactly("Bret", "Jude");

            KeyValue<Windowed<String>, KafkaPersonGroup> pattonAggregation = iterator.next();
            assertThat(pattonAggregation.key.key()).isEqualTo("Patton");
            assertThat(pattonAggregation.key.window().startTime()).isEqualTo("2000-01-01T01:00:00.00Z");
            assertThat(pattonAggregation.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(pattonAggregation.value.getFirstNameByLastName().get("Patton")).containsExactly("Jiao");
        }
    }

    @Test
    void shouldNotAggregateFirstNamesByLastNameWhenTimeWindowIsNotRespected() {
        KafkaPerson personOne = buildKafkaPersonValue("Bret", "Holman");
        KafkaPerson personTwo = buildKafkaPersonValue("Aaran", "Abbott");
        KafkaPerson personThree = buildKafkaPersonValue("Brendan", "Abbott");

        Instant start = Instant.parse("2000-01-01T01:00:00.00Z");
        inputTopic.pipeInput(new TestRecord<>("1", personOne, start));
        inputTopic.pipeInput(new TestRecord<>("2", personTwo, start.plus(4, ChronoUnit.MINUTES)));
        inputTopic.pipeInput(new TestRecord<>("3", personThree, start.plus(6, ChronoUnit.MINUTES)));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(3);
        assertThat(results.get(0).key).isEqualTo("Holman@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Holman")).containsExactly("Bret");

        assertThat(results.get(1).key).isEqualTo("Abbott@2000-01-01T01:00:00Z->2000-01-01T01:05:00Z");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

        assertThat(results.get(2).key).isEqualTo("Abbott@2000-01-01T01:05:00Z->2000-01-01T01:10:00Z");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Abbott")).containsExactly("Brendan");

        WindowStore<String, KafkaPersonGroup> stateStore = testDriver.getWindowStore(PERSON_AGGREGATE_TUMBLING_WINDOW_STATE_STORE);

        try (KeyValueIterator<Windowed<String>, KafkaPersonGroup> iterator = stateStore.all()) {
            KeyValue<Windowed<String>, KafkaPersonGroup> secondWindowAggregationAbbott = iterator.next();
            assertThat(secondWindowAggregationAbbott.key.key()).isEqualTo("Abbott");
            assertThat(secondWindowAggregationAbbott.key.window().startTime()).isEqualTo("2000-01-01T01:00:00.00Z");
            assertThat(secondWindowAggregationAbbott.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(secondWindowAggregationAbbott.value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

            KeyValue<Windowed<String>, KafkaPersonGroup> firstWindowAggregationHolman = iterator.next();
            assertThat(firstWindowAggregationHolman.key.key()).isEqualTo("Holman");
            assertThat(firstWindowAggregationHolman.key.window().startTime()).isEqualTo("2000-01-01T01:00:00.00Z");
            assertThat(firstWindowAggregationHolman.key.window().endTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(firstWindowAggregationHolman.value.getFirstNameByLastName().get("Holman")).containsExactly("Bret");

            KeyValue<Windowed<String>, KafkaPersonGroup> secondWindowAggregation = iterator.next();
            assertThat(secondWindowAggregation.key.key()).isEqualTo("Abbott");
            assertThat(secondWindowAggregation.key.window().startTime()).isEqualTo("2000-01-01T01:05:00.00Z");
            assertThat(secondWindowAggregation.key.window().endTime()).isEqualTo("2000-01-01T01:10:00.00Z");
            assertThat(secondWindowAggregation.value.getFirstNameByLastName().get("Abbott")).containsExactly("Brendan");
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
