package io.lgr.quickstarts.streams.cogroup;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.avro.KafkaPersonGroup;
import io.lgr.quickstarts.streams.cogroup.app.KafkaStreamsCogroupTopology;
import io.lgr.quickstarts.streams.cogroup.constants.StateStore;
import io.lgr.quickstarts.streams.cogroup.constants.Topic;
import io.lgr.quickstarts.streams.cogroup.serdes.CustomSerdes;
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

import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsCogroupTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopicOne;
    private TestInputTopic<String, KafkaPerson> inputTopicTwo;
    private TestOutputTopic<String, KafkaPersonGroup> outputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-cogroup-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" +  getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsCogroupTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.ofEpochMilli(1577836800000L));

        inputTopicOne = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        inputTopicTwo = testDriver.createInputTopic(Topic.PERSON_TOPIC_TWO.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        outputTopic = testDriver.createOutputTopic(Topic.PERSON_COGROUP_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPersonGroup>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(getClass().getName());
    }

    @Test
    void shouldAggregateFirstNamesByLastNameStreamOneOnly() {
        inputTopicOne.pipeKeyValueList(buildFirstKafkaPersonRecords());

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("Abbott");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott")).hasSize(1);
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott").get(0)).isEqualTo("Aaran");

        assertThat(results.get(1).key).isEqualTo("Abbott");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott")).hasSize(2);
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott").get(0)).isEqualTo("Aaran");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott").get(1)).isEqualTo("Brendan");

        assertThat(results.get(2).key).isEqualTo("Holman");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Holman")).hasSize(1);
        assertThat(results.get(2).value.getFirstNameByLastName().get("Holman").get(0)).isEqualTo("Bret");

        KeyValueStore<String, ValueAndTimestamp<KafkaPersonGroup>> stateStore = testDriver.getTimestampedKeyValueStore(StateStore.PERSON_COGROUP_AGGREGATE_STATE_STORE.toString());

        assertThat(stateStore.get("Abbott").value().getFirstNameByLastName().get("Abbott")).contains("Aaran", "Brendan");
        assertThat(stateStore.get("Holman").value().getFirstNameByLastName().get("Holman")).contains("Bret");
    }

    @Test
    void shouldAggregateFirstNamesByLastNameStreamTwoOnly() {
        inputTopicTwo.pipeKeyValueList(buildFirstKafkaPersonRecords());

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();
        assertThat(results.get(0).key).isEqualTo("Abbott");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott")).hasSize(1);
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott").get(0)).isEqualTo("Aaran");

        assertThat(results.get(1).key).isEqualTo("Abbott");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott")).hasSize(2);
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott").get(0)).isEqualTo("Aaran");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott").get(1)).isEqualTo("Brendan");

        assertThat(results.get(2).key).isEqualTo("Holman");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Holman")).hasSize(1);
        assertThat(results.get(2).value.getFirstNameByLastName().get("Holman").get(0)).isEqualTo("Bret");

        KeyValueStore<String, ValueAndTimestamp<KafkaPersonGroup>> stateStore = testDriver.getTimestampedKeyValueStore(StateStore.PERSON_COGROUP_AGGREGATE_STATE_STORE.toString());

        assertThat(stateStore.get("Abbott").value().getFirstNameByLastName().get("Abbott")).contains("Aaran", "Brendan");
        assertThat(stateStore.get("Holman").value().getFirstNameByLastName().get("Holman")).contains("Bret");
    }

    @Test
    void shouldAggregateFirstNamesByLastNameBothCogroupedStreams() {
        inputTopicOne.pipeKeyValueList(buildFirstKafkaPersonRecords());
        inputTopicTwo.pipeKeyValueList(buildSecondKafkaPersonRecords());

        List<KeyValue<String, KafkaPersonGroup>> results = outputTopic.readKeyValuesToList();
        assertThat(results.get(0).key).isEqualTo("Abbott");
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott")).hasSize(1);
        assertThat(results.get(0).value.getFirstNameByLastName().get("Abbott").get(0)).isEqualTo("Aaran");

        assertThat(results.get(1).key).isEqualTo("Abbott");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott")).hasSize(2);
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott").get(0)).isEqualTo("Aaran");
        assertThat(results.get(1).value.getFirstNameByLastName().get("Abbott").get(1)).isEqualTo("Brendan");

        assertThat(results.get(2).key).isEqualTo("Holman");
        assertThat(results.get(2).value.getFirstNameByLastName().get("Holman")).hasSize(1);
        assertThat(results.get(2).value.getFirstNameByLastName().get("Holman").get(0)).isEqualTo("Bret");

        assertThat(results.get(3).key).isEqualTo("Abbott");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Abbott")).hasSize(3);
        assertThat(results.get(3).value.getFirstNameByLastName().get("Abbott").get(0)).isEqualTo("Aaran");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Abbott").get(1)).isEqualTo("Brendan");
        assertThat(results.get(3).value.getFirstNameByLastName().get("Abbott").get(2)).isEqualTo("Daimhin");

        assertThat(results.get(4).key).isEqualTo("Holman");
        assertThat(results.get(4).value.getFirstNameByLastName().get("Holman")).hasSize(2);
        assertThat(results.get(4).value.getFirstNameByLastName().get("Holman").get(0)).isEqualTo("Bret");
        assertThat(results.get(4).value.getFirstNameByLastName().get("Holman").get(1)).isEqualTo("Jude");

        assertThat(results.get(5).key).isEqualTo("Wyatt");
        assertThat(results.get(5).value.getFirstNameByLastName().get("Wyatt")).hasSize(1);
        assertThat(results.get(5).value.getFirstNameByLastName().get("Wyatt").get(0)).isEqualTo("Kacey");

        KeyValueStore<String, ValueAndTimestamp<KafkaPersonGroup>> stateStore = testDriver.getTimestampedKeyValueStore(StateStore.PERSON_COGROUP_AGGREGATE_STATE_STORE.toString());

        assertThat(stateStore.get("Abbott").value().getFirstNameByLastName().get("Abbott")).contains("Aaran", "Brendan", "Daimhin");
        assertThat(stateStore.get("Holman").value().getFirstNameByLastName().get("Holman")).contains("Bret", "Jude");
        assertThat(stateStore.get("Wyatt").value().getFirstNameByLastName().get("Wyatt")).contains("Kacey");
    }

    private List<KeyValue<String, KafkaPerson>> buildFirstKafkaPersonRecords() {
        return Arrays.asList(
                KeyValue.pair("1", KafkaPerson.newBuilder().setId(1L).setFirstName("Aaran").setLastName("Abbott").setBirthDate(Instant.now()).build()),
                KeyValue.pair("2", KafkaPerson.newBuilder().setId(2L).setFirstName("Brendan").setLastName("Abbott").setBirthDate(Instant.now()).build()),
                KeyValue.pair("3", KafkaPerson.newBuilder().setId(3L).setFirstName("Bret").setLastName("Holman").setBirthDate(Instant.now()).build())
        );
    }

    private List<KeyValue<String, KafkaPerson>> buildSecondKafkaPersonRecords() {
        return Arrays.asList(
                KeyValue.pair("4", KafkaPerson.newBuilder().setId(1L).setFirstName("Daimhin").setLastName("Abbott").setBirthDate(Instant.now()).build()),
                KeyValue.pair("5", KafkaPerson.newBuilder().setId(2L).setFirstName("Jude").setLastName("Holman").setBirthDate(Instant.now()).build()),
                KeyValue.pair("6", KafkaPerson.newBuilder().setId(3L).setFirstName("Kacey").setLastName("Wyatt").setBirthDate(Instant.now()).build())
        );
    }
}
