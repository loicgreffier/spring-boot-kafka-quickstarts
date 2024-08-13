package io.github.loicgreffier.streams.left.join.stream.table;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.left.join.stream.table.constant.Topic.COUNTRY_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.table.constant.Topic.PERSON_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.table.constant.Topic.PERSON_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.table.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinPersonCountry;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.left.join.stream.table.app.KafkaStreamsTopology;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsLeftJoinStreamTableApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsLeftJoinStreamTableApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> personInputTopic;
    private TestInputTopic<String, KafkaCountry> countryInputTopic;
    private TestOutputTopic<String, KafkaPerson> rekeyPersonOutputTopic;
    private TestOutputTopic<String, KafkaJoinPersonCountry> joinOutputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-left-join-stream-table-test");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(
            streamsBuilder.build(),
            properties,
            Instant.parse("2000-01-01T01:00:00Z")
        );

        // Create Serde for input and output topics
        Serde<KafkaPerson> personSerde = new SpecificAvroSerde<>();
        Serde<KafkaCountry> countrySerde = new SpecificAvroSerde<>();
        Serde<KafkaJoinPersonCountry> joinPersonCountrySerde = new SpecificAvroSerde<>();
        Map<String, String> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        personSerde.configure(config, false);
        countrySerde.configure(config, false);
        joinPersonCountrySerde.configure(config, false);

        personInputTopic = testDriver.createInputTopic(PERSON_TOPIC, new StringSerializer(), personSerde.serializer());
        countryInputTopic = testDriver.createInputTopic(
            COUNTRY_TOPIC,
            new StringSerializer(),
            countrySerde.serializer()
        );
        rekeyPersonOutputTopic = testDriver.createOutputTopic(
            "streams-left-join-stream-table-test-" + PERSON_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC + "-repartition",
            new StringDeserializer(),
            personSerde.deserializer()
        );
        joinOutputTopic = testDriver.createOutputTopic(
            PERSON_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC,
            new StringDeserializer(),
            joinPersonCountrySerde.deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldRekey() {
        KafkaPerson person = buildKafkaPerson();
        personInputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaPerson>> results = rekeyPersonOutputTopic.readKeyValuesToList();

        assertEquals(KeyValue.pair("US", person), results.get(0));
    }

    @Test
    void shouldJoinPersonToCountry() {
        KafkaCountry country = buildKafkaCountry();
        countryInputTopic.pipeInput("US", country);

        KafkaPerson person = buildKafkaPerson();
        personInputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaJoinPersonCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("US", results.get(0).key);
        assertEquals(person, results.get(0).value.getPerson());
        assertEquals(country, results.get(0).value.getCountry());
    }

    @Test
    void shouldEmitValueEvenIfNoCountry() {
        KafkaPerson person = buildKafkaPerson();
        personInputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaJoinPersonCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("US", results.get(0).key);
        assertEquals(person, results.get(0).value.getPerson());
        assertNull(results.get(0).value.getCountry());
    }

    private KafkaPerson buildKafkaPerson() {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName("Homer")
            .setLastName("Simpson")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .setNationality(CountryCode.US)
            .build();
    }

    private KafkaCountry buildKafkaCountry() {
        return KafkaCountry.newBuilder()
            .setCode(CountryCode.US)
            .setName("United States")
            .setCapital("Washington")
            .setOfficialLanguage("English")
            .build();
    }
}
