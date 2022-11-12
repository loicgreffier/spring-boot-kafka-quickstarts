package io.lgr.quickstarts.streams.reduce;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.lgr.quickstarts.avro.CountryCode;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.reduce.app.KafkaStreamsReduceTopology;
import io.lgr.quickstarts.streams.reduce.constants.StateStore;
import io.lgr.quickstarts.streams.reduce.constants.Topic;
import io.lgr.quickstarts.streams.reduce.serdes.CustomSerdes;
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

class KafkaStreamsReduceTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPerson> outputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-reduce-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" + getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsReduceTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.ofEpochMilli(1577836800000L));

        inputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        outputTopic = testDriver.createOutputTopic(Topic.PERSON_REDUCE_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(getClass().getName());
    }

    @Test
    void shouldReduceByNationalityAndKeepMaxAge() {
        inputTopic.pipeKeyValueList(buildKafkaPersonRecords());

        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();
        assertThat(results.get(0).key).isEqualTo(CountryCode.FR.toString());
        assertThat(results.get(0).value.getId()).isEqualTo(1);

        assertThat(results.get(1).key).isEqualTo(CountryCode.CH.toString());
        assertThat(results.get(1).value.getId()).isEqualTo(2);

        assertThat(results.get(2).key).isEqualTo(CountryCode.FR.toString());
        assertThat(results.get(2).value.getId()).isEqualTo(1);

        assertThat(results.get(3).key).isEqualTo(CountryCode.ES.toString());
        assertThat(results.get(3).value.getId()).isEqualTo(4);

        assertThat(results.get(4).key).isEqualTo(CountryCode.FR.toString());
        assertThat(results.get(4).value.getId()).isEqualTo(5);

        assertThat(results.get(5).key).isEqualTo(CountryCode.GB.toString());
        assertThat(results.get(5).value.getId()).isEqualTo(6);

        assertThat(results.get(6).key).isEqualTo(CountryCode.GB.toString());
        assertThat(results.get(6).value.getId()).isEqualTo(6);

        assertThat(results.get(7).key).isEqualTo(CountryCode.CH.toString());
        assertThat(results.get(7).value.getId()).isEqualTo(8);

        assertThat(results.get(8).key).isEqualTo(CountryCode.DE.toString());
        assertThat(results.get(8).value.getId()).isEqualTo(9);

        assertThat(results.get(9).key).isEqualTo(CountryCode.IT.toString());
        assertThat(results.get(9).value.getId()).isEqualTo(10);

        assertThat(results.get(10).key).isEqualTo(CountryCode.DE.toString());
        assertThat(results.get(10).value.getId()).isEqualTo(9);

        KeyValueStore<String, ValueAndTimestamp<KafkaPerson>> stateStore = testDriver.getTimestampedKeyValueStore(StateStore.PERSON_REDUCE_STATE_STORE.toString());

        assertThat(stateStore.get(CountryCode.FR.toString()).value().getId()).isEqualTo(5);
        assertThat(stateStore.get(CountryCode.CH.toString()).value().getId()).isEqualTo(8);
        assertThat(stateStore.get(CountryCode.ES.toString()).value().getId()).isEqualTo(4);
        assertThat(stateStore.get(CountryCode.GB.toString()).value().getId()).isEqualTo(6);
        assertThat(stateStore.get(CountryCode.DE.toString()).value().getId()).isEqualTo(9);
        assertThat(stateStore.get(CountryCode.IT.toString()).value().getId()).isEqualTo(10);
    }

    private List<KeyValue<String, KafkaPerson>> buildKafkaPersonRecords() {
        return Arrays.asList(
                KeyValue.pair("1", KafkaPerson.newBuilder().setId(1L).setFirstName("Aaran").setLastName("Abbott")
                        .setBirthDate(Instant.parse("1956-08-29T18:35:24.00Z")).setNationality(CountryCode.FR).build()),
                KeyValue.pair("2", KafkaPerson.newBuilder().setId(2L).setFirstName("Brendan").setLastName("Abbott")
                        .setBirthDate(Instant.parse("1995-12-15T23:06:22.00Z")).setNationality(CountryCode.CH).build()),
                KeyValue.pair("3", KafkaPerson.newBuilder().setId(3L).setFirstName("Bret").setLastName("Holman")
                        .setBirthDate(Instant.parse("1994-11-09T08:08:50.00Z")).setNationality(CountryCode.FR).build()),
                KeyValue.pair("4", KafkaPerson.newBuilder().setId(4L).setFirstName("Daimhin").setLastName("Abbott")
                        .setBirthDate(Instant.parse("1971-06-15T14:15:01.00Z")).setNationality(CountryCode.ES).build()),
                KeyValue.pair("5", KafkaPerson.newBuilder().setId(5L).setFirstName("Glen").setLastName("Abbott")
                        .setBirthDate(Instant.parse("1928-10-27T18:25:31Z")).setNationality(CountryCode.FR).build()),
                KeyValue.pair("6", KafkaPerson.newBuilder().setId(6L).setFirstName("Jiao").setLastName("Patton")
                        .setBirthDate(Instant.parse("1986-02-02T04:58:01.00Z")).setNationality(CountryCode.GB).build()),
                KeyValue.pair("7", KafkaPerson.newBuilder().setId(7L).setFirstName("Matteo").setLastName("Patton")
                        .setBirthDate(Instant.parse("1996-05-26T04:52:06Z")).setNationality(CountryCode.GB).build()),
                KeyValue.pair("8", KafkaPerson.newBuilder().setId(8L).setFirstName("Acevedo").setLastName("Holman")
                        .setBirthDate(Instant.parse("1953-02-08T01:34:56Z")).setNationality(CountryCode.CH).build()),
                KeyValue.pair("9", KafkaPerson.newBuilder().setId(9L).setFirstName("Bennett").setLastName("Patton")
                        .setBirthDate(Instant.parse("1921-01-06T19:32:07Z")).setNationality(CountryCode.DE).build()),
                KeyValue.pair("10", KafkaPerson.newBuilder().setId(10L).setFirstName("Donaldson").setLastName("Holman")
                        .setBirthDate(Instant.parse("2006-04-07T09:05:36Z")).setNationality(CountryCode.IT).build()),
                KeyValue.pair("11", KafkaPerson.newBuilder().setId(11L).setFirstName("Pardeepraj").setLastName("Holman")
                        .setBirthDate(Instant.parse("1958-03-09T09:40:44Z")).setNationality(CountryCode.DE).build())
        );
    }
}
