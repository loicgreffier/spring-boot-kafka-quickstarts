package io.github.loicgreffier.streams.cogroup;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.cogroup.app.KafkaStreamsCogroupTopology;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.github.loicgreffier.streams.cogroup.constants.StateStore.PERSON_COGROUP_AGGREGATE_STATE_STORE;
import static io.github.loicgreffier.streams.cogroup.constants.Topic.*;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsCogroupTest {
    private static final String SCHEMA_REGISTRY_SCOPE = KafkaStreamsCogroupTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopicOne;
    private TestInputTopic<String, KafkaPerson> inputTopicTwo;
    private TestOutputTopic<String, KafkaPersonGroup> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-cogroup-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsCogroupTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        // Create Serde for input and output topics
        Serde<KafkaPerson> personSerde = new SpecificAvroSerde<>();
        Serde<KafkaPersonGroup> personGroupSerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        personGroupSerde.configure(config, false);

        inputTopicOne = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        inputTopicTwo = testDriver.createInputTopic(PERSON_TOPIC_TWO, new StringSerializer(), personSerde.serializer());
        outputTopic = testDriver.createOutputTopic(PERSON_COGROUP_TOPIC, new StringDeserializer(), personGroupSerde.deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldAggregateFirstNamesByLastNameStreamOne() {
        inputTopicOne.pipeKeyValueList(buildFirstKafkaPersonRecords());

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(3);
        assertThat(results.get(0).key).isEqualTo("Abbott");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

        assertThat(results.get(1).key).isEqualTo("Abbott");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan");

        assertThat(results.get(2).key).isEqualTo("Holman");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Holman")).containsExactly("Bret");

        KeyValueStore<String, ValueAndTimestamp<KafkaPersonGroup>> stateStore = testDriver.getTimestampedKeyValueStore(PERSON_COGROUP_AGGREGATE_STATE_STORE);

        assertThat(stateStore.get("Abbott").value().getFirstNameByLastName().get("Abbott")).contains("Aaran", "Brendan");
        assertThat(stateStore.get("Holman").value().getFirstNameByLastName().get("Holman")).contains("Bret");
    }

    @Test
    void shouldAggregateFirstNamesByLastNameStreamTwo() {
        inputTopicTwo.pipeKeyValueList(buildFirstKafkaPersonRecords());

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(3);
        assertThat(results.get(0).key).isEqualTo("Abbott");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

        assertThat(results.get(1).key).isEqualTo("Abbott");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan");

        assertThat(results.get(2).key).isEqualTo("Holman");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Holman")).containsExactly("Bret");

        KeyValueStore<String, ValueAndTimestamp<KafkaPersonGroup>> stateStore = testDriver.getTimestampedKeyValueStore(PERSON_COGROUP_AGGREGATE_STATE_STORE);

        assertThat(stateStore.get("Abbott").value().getFirstNameByLastName().get("Abbott")).contains("Aaran", "Brendan");
        assertThat(stateStore.get("Holman").value().getFirstNameByLastName().get("Holman")).contains("Bret");
    }

    @Test
    void shouldAggregateFirstNamesByLastNameBothCogroupedStreams() {
        inputTopicOne.pipeKeyValueList(buildFirstKafkaPersonRecords());
        inputTopicTwo.pipeKeyValueList(buildSecondKafkaPersonRecords());

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(6);
        assertThat(results.get(0).key).isEqualTo("Abbott");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran");

        assertThat(results.get(1).key).isEqualTo("Abbott");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan");

        assertThat(results.get(2).key).isEqualTo("Holman");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Holman")).containsExactly("Bret");

        assertThat(results.get(3).key).isEqualTo("Abbott");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan", "Daimhin");

        assertThat(results.get(4).key).isEqualTo("Holman");
        assertThat(results.get(4).value.getFirstNameByLastName().get("Holman")).containsExactly("Bret", "Jude");

        assertThat(results.get(5).key).isEqualTo("Wyatt");
        assertThat(results.get(5).value.getFirstNameByLastName().get("Wyatt")).containsExactly("Kacey");

        KeyValueStore<String, ValueAndTimestamp<KafkaPersonGroup>> stateStore = testDriver.getTimestampedKeyValueStore(PERSON_COGROUP_AGGREGATE_STATE_STORE);

        assertThat(stateStore.get("Abbott").value().getFirstNameByLastName().get("Abbott")).contains("Aaran", "Brendan", "Daimhin");
        assertThat(stateStore.get("Holman").value().getFirstNameByLastName().get("Holman")).contains("Bret", "Jude");
        assertThat(stateStore.get("Wyatt").value().getFirstNameByLastName().get("Wyatt")).contains("Kacey");
    }

    private List<KeyValue<String, KafkaPerson>> buildFirstKafkaPersonRecords() {
        return Arrays.asList(
                KeyValue.pair("1", KafkaPerson.newBuilder().setId(1L).setFirstName("Aaran").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("2", KafkaPerson.newBuilder().setId(2L).setFirstName("Brendan").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("3", KafkaPerson.newBuilder().setId(3L).setFirstName("Bret").setLastName("Holman").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build())
        );
    }

    private List<KeyValue<String, KafkaPerson>> buildSecondKafkaPersonRecords() {
        return Arrays.asList(
                KeyValue.pair("4", KafkaPerson.newBuilder().setId(1L).setFirstName("Daimhin").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("5", KafkaPerson.newBuilder().setId(2L).setFirstName("Jude").setLastName("Holman").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("6", KafkaPerson.newBuilder().setId(3L).setFirstName("Kacey").setLastName("Wyatt").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build())
        );
    }
}
