package io.github.loicgreffier.streams.join.stream.table;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.COUNTRY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.PERSON_COUNTRY_JOIN_STREAM_TABLE_TOPIC;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.PERSON_JOIN_STREAM_TABLE_REKEY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinPersonCountry;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.join.stream.table.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.join.stream.table.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

class KafkaStreamsJoinStreamTableApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsJoinStreamTableApplicationTest.class.getName();
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
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-join-stream-table-test");
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.setProperty(STATE_DIR_CONFIG, STATE_DIR);
        properties.setProperty(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        // Create SerDes
        Map<String, String> config = Map.of(SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        SerdesUtils.setSerdesConfig(config);

        // Create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(
            streamsBuilder.build(),
            properties,
            Instant.parse("2000-01-01T01:00:00Z")
        );

        personInputTopic = testDriver.createInputTopic(
            PERSON_TOPIC,
            new StringSerializer(),
            SerdesUtils.<KafkaPerson>getValueSerdes().serializer()
        );
        countryInputTopic = testDriver.createInputTopic(
            COUNTRY_TOPIC,
            new StringSerializer(),
            SerdesUtils.<KafkaCountry>getValueSerdes().serializer()
        );
        rekeyPersonOutputTopic = testDriver.createOutputTopic(
            "streams-join-stream-table-test-" + PERSON_JOIN_STREAM_TABLE_REKEY_TOPIC + "-repartition",
            new StringDeserializer(),
            SerdesUtils.<KafkaPerson>getValueSerdes().deserializer()
        );
        joinOutputTopic = testDriver.createOutputTopic(
            PERSON_COUNTRY_JOIN_STREAM_TABLE_TOPIC,
            new StringDeserializer(),
            SerdesUtils.<KafkaJoinPersonCountry>getValueSerdes().deserializer()
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

        assertEquals(KeyValue.pair("US", person), results.getFirst());
    }

    @Test
    void shouldJoinPersonToCountry() {
        KafkaCountry country = buildKafkaCountry();
        countryInputTopic.pipeInput("US", country);

        KafkaPerson person = buildKafkaPerson();
        personInputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaJoinPersonCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertEquals("US", results.getFirst().key);
        assertEquals(person, results.getFirst().value.getPerson());
        assertEquals(country, results.getFirst().value.getCountry());
    }

    @Test
    void shouldNotJoinWhenNoCountry() {
        personInputTopic.pipeInput("1", buildKafkaPerson());
        List<KeyValue<String, KafkaJoinPersonCountry>> results = joinOutputTopic.readKeyValuesToList();

        assertTrue(results.isEmpty());
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
