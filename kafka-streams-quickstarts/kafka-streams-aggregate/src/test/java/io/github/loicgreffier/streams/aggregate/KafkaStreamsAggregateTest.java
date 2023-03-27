package io.github.loicgreffier.streams.aggregate;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.app.KafkaStreamsAggregateTopology;
import io.github.loicgreffier.streams.aggregate.constants.StateStore;
import io.github.loicgreffier.streams.aggregate.constants.Topic;
import io.github.loicgreffier.streams.aggregate.serdes.CustomSerdes;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsAggregateTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPersonGroup> outputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-aggregate-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" + getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + getClass().getName());
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsAggregateTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        inputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        outputTopic = testDriver.createOutputTopic(Topic.PERSON_AGGREGATE_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPersonGroup>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope("mock://" + getClass().getName());
    }

    @Test
    void shouldAggregateFirstNamesByLastName() {
        inputTopic.pipeKeyValueList(buildKafkaPersonRecords());

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

        assertThat(results.get(4).key).isEqualTo("Patton");
        assertThat(results.get(4).value.getFirstNameByLastName().get("Patton")).containsExactly("Jiao");

        assertThat(results.get(5).key).isEqualTo("Holman");
        assertThat(results.get(5).value.getFirstNameByLastName().get("Holman")).containsExactly("Bret", "Jude");

        KeyValueStore<String, ValueAndTimestamp<KafkaPersonGroup>> stateStore = testDriver.getTimestampedKeyValueStore(StateStore.PERSON_AGGREGATE_STATE_STORE.toString());

        assertThat(stateStore.get("Abbott").value().getFirstNameByLastName().get("Abbott")).containsExactly("Aaran", "Brendan", "Daimhin");
        assertThat(stateStore.get("Holman").value().getFirstNameByLastName().get("Holman")).containsExactly("Bret", "Jude");
        assertThat(stateStore.get("Patton").value().getFirstNameByLastName().get("Patton")).containsExactly("Jiao");
    }

    private List<KeyValue<String, KafkaPerson>> buildKafkaPersonRecords() {
        return List.of(
                KeyValue.pair("1", KafkaPerson.newBuilder().setId(1L).setFirstName("Aaran").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("2", KafkaPerson.newBuilder().setId(2L).setFirstName("Brendan").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("3", KafkaPerson.newBuilder().setId(3L).setFirstName("Bret").setLastName("Holman").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("4", KafkaPerson.newBuilder().setId(2L).setFirstName("Daimhin").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("5", KafkaPerson.newBuilder().setId(3L).setFirstName("Jiao").setLastName("Patton").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build()),
                KeyValue.pair("6", KafkaPerson.newBuilder().setId(3L).setFirstName("Jude").setLastName("Holman").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).build())
        );
    }
}
