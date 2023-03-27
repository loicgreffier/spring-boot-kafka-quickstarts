package io.github.loicgreffier.streams.left.join.stream.table;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinPersonCountry;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.left.join.stream.table.app.KafkaStreamsLeftJoinStreamTableTopology;
import io.github.loicgreffier.streams.left.join.stream.table.constants.Topic;
import io.github.loicgreffier.streams.left.join.stream.table.serdes.CustomSerdes;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsLeftJoinStreamTableTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> personInputTopic;
    private TestInputTopic<String, KafkaCountry> countryInputTopic;
    private TestOutputTopic<String, KafkaPerson> personRekeyOutputTopic;
    private TestOutputTopic<String, KafkaJoinPersonCountry> joinOutputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-left-join-stream-table-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" +  getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + getClass().getName());
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsLeftJoinStreamTableTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        personInputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        countryInputTopic = testDriver.createInputTopic(Topic.COUNTRY_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaCountry>getValueSerdes().serializer());

        personRekeyOutputTopic = testDriver.createOutputTopic("streams-left-join-stream-table-test-" + Topic.PERSON_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC + "-repartition", new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());

        joinOutputTopic = testDriver.createOutputTopic(Topic.PERSON_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaJoinPersonCountry>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope("mock://" + getClass().getName());
    }

    @Test
    void shouldRekey() {
        KafkaPerson person = buildKafkaPersonValue();
        personInputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaPerson>> results = personRekeyOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo("FR");
        assertThat(results.get(0).value).isEqualTo(person);
    }

    @Test
    void shouldJoinPersonToCountry() {
        KafkaCountry country = buildKafkaCountryValue();
        KafkaPerson person = buildKafkaPersonValue();

        countryInputTopic.pipeInput("FR", country);
        personInputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaJoinPersonCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo("FR");
        assertThat(results.get(0).value.getPerson()).isEqualTo(person);
        assertThat(results.get(0).value.getCountry()).isEqualTo(country);
    }

    @Test
    void shouldEmitValueEvenIfNoCountry() {
        KafkaPerson person = buildKafkaPersonValue();
        personInputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaJoinPersonCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo("FR");
        assertThat(results.get(0).value.getPerson()).isEqualTo(person);
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
