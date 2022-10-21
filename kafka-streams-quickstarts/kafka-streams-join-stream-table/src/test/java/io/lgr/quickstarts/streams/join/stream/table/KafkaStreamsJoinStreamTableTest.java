package io.lgr.quickstarts.streams.join.stream.table;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.lgr.quickstarts.avro.CountryCode;
import io.lgr.quickstarts.avro.KafkaCountry;
import io.lgr.quickstarts.avro.KafkaJoinPersonCountry;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.join.stream.table.app.KafkaStreamsJoinStreamTableTopology;
import io.lgr.quickstarts.streams.join.stream.table.constants.Topic;
import io.lgr.quickstarts.streams.join.stream.table.serdes.CustomSerdes;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsJoinStreamTableTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> personInputTopic;
    private TestInputTopic<String, KafkaCountry> countryInputTopic;
    private TestOutputTopic<String, KafkaPerson> rekeyPersonOutputTopic;
    private TestOutputTopic<String, KafkaCountry> rekeyCountryOutputTopic;
    private TestOutputTopic<String, KafkaJoinPersonCountry> joinOutputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-map-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsJoinStreamTableTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.ofEpochMilli(1577836800000L));

        personInputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        countryInputTopic = testDriver.createInputTopic(Topic.COUNTRY_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaCountry>getValueSerdes().serializer());

        rekeyPersonOutputTopic = testDriver.createOutputTopic(Topic.PERSON_REKEY_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());

        rekeyCountryOutputTopic = testDriver.createOutputTopic(Topic.COUNTRY_REKEY_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaCountry>getValueSerdes().deserializer());

        joinOutputTopic = testDriver.createOutputTopic(Topic.JOIN_PERSON_COUNTRY_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaJoinPersonCountry>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
        FileUtils.deleteQuietly(new File(STATE_DIR));
        MockSchemaRegistry.dropScope(this.getClass().getName());
    }

    @Test
    void testRekeyPerson() {
        personInputTopic.pipeInput("1", buildPerson());

        List<KeyValue<String, KafkaPerson>> results = rekeyPersonOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo("FR");
        assertThat(results.get(0).value.getId()).isEqualTo(1L);
        assertThat(results.get(0).value.getFirstName()).isEqualTo("First name");
        assertThat(results.get(0).value.getLastName()).isEqualTo("Last name");
    }

    @Test
    void testRekeyCountry() {
        countryInputTopic.pipeInput("1", buildCountry());

        List<KeyValue<String, KafkaCountry>> results = rekeyCountryOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo("FR");
        assertThat(results.get(0).value.getName()).isEqualTo("France");
        assertThat(results.get(0).value.getCapital()).isEqualTo("Paris");
        assertThat(results.get(0).value.getOfficialLanguage()).isEqualTo("French");
    }

    @Test
    void testJoin() {
        countryInputTopic.pipeInput("1", buildCountry());
        personInputTopic.pipeInput("1", buildPerson());

        List<KeyValue<String, KafkaJoinPersonCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo("FR");
        assertThat(results.get(0).value.getPerson().getId()).isEqualTo(1L);
        assertThat(results.get(0).value.getCountry().getName()).isEqualTo("France");
    }

    private KafkaPerson buildPerson() {
        return KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("First name")
                .setLastName("Last name")
                .setBirthDate(Instant.now())
                .setNationality(CountryCode.FR)
                .build();
    }

    private KafkaCountry buildCountry() {
        return KafkaCountry.newBuilder()
                .setCode(CountryCode.FR)
                .setName("France")
                .setCapital("Paris")
                .setOfficialLanguage("French")
                .build();
    }
}
