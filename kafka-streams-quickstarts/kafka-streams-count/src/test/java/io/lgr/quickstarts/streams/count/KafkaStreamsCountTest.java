package io.lgr.quickstarts.streams.count;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.lgr.quickstarts.avro.CountryCode;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.count.app.KafkaStreamsCountTopology;
import io.lgr.quickstarts.streams.count.constants.StateStore;
import io.lgr.quickstarts.streams.count.constants.Topic;
import io.lgr.quickstarts.streams.count.serdes.CustomSerdes;
import org.apache.kafka.common.serialization.LongDeserializer;
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

class KafkaStreamsCountTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-count-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" + getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsCountTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        inputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        outputTopic = testDriver.createOutputTopic(Topic.PERSON_COUNT_TOPIC.toString(), new StringDeserializer(),
                new LongDeserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(getClass().getName());
    }

    @Test
    void shouldCountByNationality() {
        inputTopic.pipeKeyValueList(buildKafkaPersonRecords());

        List<KeyValue<String, Long>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(8);
        assertThat(results.get(0).key).isEqualTo(CountryCode.FR.toString());
        assertThat(results.get(0).value).isEqualTo(1);

        assertThat(results.get(1).key).isEqualTo(CountryCode.CH.toString());
        assertThat(results.get(1).value).isEqualTo(1);

        assertThat(results.get(2).key).isEqualTo(CountryCode.FR.toString());
        assertThat(results.get(2).value).isEqualTo(2);

        assertThat(results.get(3).key).isEqualTo(CountryCode.ES.toString());
        assertThat(results.get(3).value).isEqualTo(1);

        assertThat(results.get(4).key).isEqualTo(CountryCode.GB.toString());
        assertThat(results.get(4).value).isEqualTo(1);

        assertThat(results.get(5).key).isEqualTo(CountryCode.CH.toString());
        assertThat(results.get(5).value).isEqualTo(2);

        assertThat(results.get(6).key).isEqualTo(CountryCode.DE.toString());
        assertThat(results.get(6).value).isEqualTo(1);

        assertThat(results.get(7).key).isEqualTo(CountryCode.IT.toString());
        assertThat(results.get(7).value).isEqualTo(1);

        KeyValueStore<String, ValueAndTimestamp<Long>> stateStore = testDriver.getTimestampedKeyValueStore(StateStore.PERSON_COUNT_STATE_STORE.toString());

        assertThat(stateStore.get(CountryCode.FR.toString()).value()).isEqualTo(2);
        assertThat(stateStore.get(CountryCode.CH.toString()).value()).isEqualTo(2);
        assertThat(stateStore.get(CountryCode.ES.toString()).value()).isEqualTo(1);
        assertThat(stateStore.get(CountryCode.GB.toString()).value()).isEqualTo(1);
        assertThat(stateStore.get(CountryCode.DE.toString()).value()).isEqualTo(1);
        assertThat(stateStore.get(CountryCode.IT.toString()).value()).isEqualTo(1);
    }

    private List<KeyValue<String, KafkaPerson>> buildKafkaPersonRecords() {
        return List.of(
                KeyValue.pair("1", KafkaPerson.newBuilder().setId(1L).setFirstName("Aaran").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).setNationality(CountryCode.FR).build()),
                KeyValue.pair("2", KafkaPerson.newBuilder().setId(2L).setFirstName("Brendan").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).setNationality(CountryCode.CH).build()),
                KeyValue.pair("3", KafkaPerson.newBuilder().setId(3L).setFirstName("Bret").setLastName("Holman").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).setNationality(CountryCode.FR).build()),
                KeyValue.pair("4", KafkaPerson.newBuilder().setId(4L).setFirstName("Daimhin").setLastName("Abbott").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).setNationality(CountryCode.ES).build()),
                KeyValue.pair("5", KafkaPerson.newBuilder().setId(5L).setFirstName("Jiao").setLastName("Patton").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).setNationality(CountryCode.GB).build()),
                KeyValue.pair("6", KafkaPerson.newBuilder().setId(6L).setFirstName("Acevedo").setLastName("Holman").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).setNationality(CountryCode.CH).build()),
                KeyValue.pair("7", KafkaPerson.newBuilder().setId(7L).setFirstName("Bennett").setLastName("Patton").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).setNationality(CountryCode.DE).build()),
                KeyValue.pair("8", KafkaPerson.newBuilder().setId(8L).setFirstName("Donaldson").setLastName("Holman").setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z")).setNationality(CountryCode.IT).build())
        );
    }
}
