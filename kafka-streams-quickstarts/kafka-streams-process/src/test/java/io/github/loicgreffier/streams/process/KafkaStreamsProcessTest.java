package io.github.loicgreffier.streams.process;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import io.github.loicgreffier.streams.process.app.KafkaStreamsProcessTopology;
import io.github.loicgreffier.streams.process.constants.StateStore;
import io.github.loicgreffier.streams.process.constants.Topic;
import io.github.loicgreffier.streams.process.serdes.CustomSerdes;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
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

import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsProcessTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, Long> changelogOutputTopic;
    private TestOutputTopic<String, KafkaPersonMetadata> outputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-process-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" + getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + getClass().getName());
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsProcessTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        inputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        changelogOutputTopic = testDriver.createOutputTopic("streams-process-test-" + StateStore.PERSON_PROCESS_STATE_STORE + "-changelog", new StringDeserializer(),
                new LongDeserializer());

        outputTopic = testDriver.createOutputTopic(Topic.PERSON_PROCESS_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPersonMetadata>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope("mock://" + getClass().getName());
    }

    @Test
    void shouldProcess() {
        List<KeyValue<String, KafkaPerson>> persons = buildKafkaPersonRecords();
        inputTopic.pipeKeyValueList(persons);
        List<KeyValue<String, KafkaPersonMetadata>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(4);
        assertThat(results.get(0).key).isEqualTo("Abbott");
        assertThat(results.get(0).value.getPerson()).isEqualTo(persons.get(0).value);
        assertThat(results.get(0).value.getTopic()).isEqualTo(Topic.PERSON_TOPIC.toString());
        assertThat(results.get(0).value.getPartition()).isZero();
        assertThat(results.get(0).value.getOffset()).isZero();

        assertThat(results.get(1).key).isEqualTo("Abbott");
        assertThat(results.get(1).value.getPerson()).isEqualTo(persons.get(1).value);
        assertThat(results.get(1).value.getTopic()).isEqualTo(Topic.PERSON_TOPIC.toString());
        assertThat(results.get(1).value.getPartition()).isZero();
        assertThat(results.get(1).value.getOffset()).isEqualTo(1);

        assertThat(results.get(2).key).isEqualTo("Holman");
        assertThat(results.get(2).value.getPerson()).isEqualTo(persons.get(2).value);
        assertThat(results.get(2).value.getTopic()).isEqualTo(Topic.PERSON_TOPIC.toString());
        assertThat(results.get(2).value.getPartition()).isZero();
        assertThat(results.get(2).value.getOffset()).isEqualTo(2);

        assertThat(results.get(3).key).isEqualTo("Holman");
        assertThat(results.get(3).value.getPerson()).isEqualTo(persons.get(3).value);
        assertThat(results.get(3).value.getTopic()).isEqualTo(Topic.PERSON_TOPIC.toString());
        assertThat(results.get(3).value.getPartition()).isZero();
        assertThat(results.get(3).value.getOffset()).isEqualTo(3);

        KeyValueStore<String, Long> stateStore = testDriver.getKeyValueStore(StateStore.PERSON_PROCESS_STATE_STORE.toString());

        assertThat(stateStore.get("Abbott")).isEqualTo(2);
        assertThat(stateStore.get("Holman")).isEqualTo(2);

        List<KeyValue<String, Long>> changelogResults = changelogOutputTopic.readKeyValuesToList();

        assertThat(changelogResults).hasSize(4);
        assertThat(changelogResults.get(0).key).isEqualTo("Abbott");
        assertThat(changelogResults.get(0).value).isEqualTo(1L);
        assertThat(changelogResults.get(1).key).isEqualTo("Abbott");
        assertThat(changelogResults.get(1).value).isEqualTo(2L);
        assertThat(changelogResults.get(2).key).isEqualTo("Holman");
        assertThat(changelogResults.get(2).value).isEqualTo(1L);
        assertThat(changelogResults.get(3).key).isEqualTo("Holman");
        assertThat(changelogResults.get(3).value).isEqualTo(2L);
    }

    private List<KeyValue<String, KafkaPerson>> buildKafkaPersonRecords() {
        return List.of(
                KeyValue.pair("1", KafkaPerson.newBuilder().setId(1L).setFirstName("Aaran").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("2", KafkaPerson.newBuilder().setId(2L).setFirstName("Brendan").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("3", KafkaPerson.newBuilder().setId(3L).setFirstName("Bret").setLastName("Holman").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("6", KafkaPerson.newBuilder().setId(3L).setFirstName("Jude").setLastName("Holman").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build())
        );
    }
}
