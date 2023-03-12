package io.lgr.quickstarts.streams.left.join.stream.global.table;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.lgr.quickstarts.avro.CountryCode;
import io.lgr.quickstarts.avro.KafkaCountry;
import io.lgr.quickstarts.avro.KafkaJoinPersonCountry;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.left.join.stream.global.table.app.KafkaStreamsLeftJoinStreamGlobalTableTopology;
import io.lgr.quickstarts.streams.left.join.stream.global.table.constants.Topic;
import io.lgr.quickstarts.streams.left.join.stream.global.table.serdes.CustomSerdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsLeftJoinStreamGlobalTableTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> personInputTopic;
    private TestInputTopic<String, KafkaCountry> countryInputTopic;
    private TestOutputTopic<String, KafkaJoinPersonCountry> joinOutputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-left-join-stream-global-table-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" +  getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + getClass().getName());
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsLeftJoinStreamGlobalTableTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        personInputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        countryInputTopic = testDriver.createInputTopic(Topic.COUNTRY_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaCountry>getValueSerdes().serializer());

        joinOutputTopic = testDriver.createOutputTopic(Topic.PERSON_COUNTRY_LEFT_JOIN_STREAM_GLOBAL_TABLE_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaJoinPersonCountry>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope("mock://" + getClass().getName());
    }

    @Test
    void shouldJoinPersonToCountry() {
        KeyValue<String, KafkaCountry> country = KeyValue.pair("FR", buildKafkaCountryValue());
        KeyValue<String, KafkaPerson> person = KeyValue.pair("1", buildKafkaPersonValue());

        countryInputTopic.pipeKeyValueList(Collections.singletonList(country));
        personInputTopic.pipeKeyValueList(Collections.singletonList(person));

        List<KeyValue<String, KafkaJoinPersonCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo(person.key);
        assertThat(results.get(0).value.getPerson()).isEqualTo(person.value);
        assertThat(results.get(0).value.getCountry()).isEqualTo(country.value);
    }

    @Test
    void shouldEmitValueEvenIfNoCountry() {
        KeyValue<String, KafkaPerson> person = KeyValue.pair("1", buildKafkaPersonValue());
        personInputTopic.pipeKeyValueList(Collections.singletonList(person));

        List<KeyValue<String, KafkaJoinPersonCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo(person.key);
        assertThat(results.get(0).value.getPerson()).isEqualTo(person.value);
        assertThat(results.get(0).value.getCountry()).isNull();
    }

    private KafkaPerson buildKafkaPersonValue() {
        return KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("First name")
                .setLastName("Last name")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .setNationality(CountryCode.FR)
                .build();
    }

    private KafkaCountry buildKafkaCountryValue() {
        return KafkaCountry.newBuilder()
                .setCode(CountryCode.FR)
                .setName("France")
                .setCapital("Paris")
                .setOfficialLanguage("French")
                .build();
    }
}
