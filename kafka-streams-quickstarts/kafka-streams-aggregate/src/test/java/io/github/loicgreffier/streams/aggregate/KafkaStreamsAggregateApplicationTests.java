package io.github.loicgreffier.streams.aggregate;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.app.KafkaStreamsTopology;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.github.loicgreffier.streams.aggregate.constants.StateStore.PERSON_AGGREGATE_STATE_STORE;
import static io.github.loicgreffier.streams.aggregate.constants.Topic.PERSON_AGGREGATE_TOPIC;
import static io.github.loicgreffier.streams.aggregate.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsAggregateApplicationTests {
    private static final String SCHEMA_REGISTRY_SCOPE = KafkaStreamsAggregateApplicationTests.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPersonGroup> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-aggregate-test");
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
        outputTopic = testDriver.createOutputTopic(PERSON_AGGREGATE_TOPIC, new StringDeserializer(), personGroupSerde.deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldAggregate() {
        inputTopic.pipeInput("1", buildKafkaPerson("Aaran", "Allen"));
        inputTopic.pipeInput("2", buildKafkaPerson("Bret", "Wise"));
        inputTopic.pipeInput("3", buildKafkaPerson("Brendan", "Allen"));
        inputTopic.pipeInput("4", buildKafkaPerson("Jude", "Wise"));

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Allen");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran");

        assertThat(results.get(1).key).isEqualTo("Wise");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Wise")).containsExactly("Bret");

        assertThat(results.get(2).key).isEqualTo("Allen");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Allen")).containsExactly("Aaran", "Brendan");

        assertThat(results.get(3).key).isEqualTo("Wise");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Wise")).containsExactly("Bret", "Jude");

        // Explore state store
        KeyValueStore<String, KafkaPersonGroup> stateStore = testDriver.getKeyValueStore(PERSON_AGGREGATE_STATE_STORE);

        assertThat(stateStore.get("Allen").getFirstNameByLastName().get("Allen")).containsExactly("Aaran", "Brendan");
        assertThat(stateStore.get("Wise").getFirstNameByLastName().get("Wise")).containsExactly("Bret", "Jude");
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
