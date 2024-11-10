package io.github.loicgreffier.streams.reduce;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.reduce.constant.StateStore.PERSON_REDUCE_STORE;
import static io.github.loicgreffier.streams.reduce.constant.Topic.PERSON_REDUCE_TOPIC;
import static io.github.loicgreffier.streams.reduce.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.reduce.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.reduce.serdes.SerdesUtils;
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
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsReduceApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsReduceApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPerson> outputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-reduce-test");
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

        inputTopic = testDriver.createInputTopic(
            PERSON_TOPIC,
            new StringSerializer(),
            SerdesUtils.<KafkaPerson>getValueSerdes().serializer()
        );
        outputTopic = testDriver.createOutputTopic(
            PERSON_REDUCE_TOPIC,
            new StringDeserializer(),
            SerdesUtils.<KafkaPerson>getValueSerdes().deserializer()
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldReduceByNationalityAndKeepOldest() {
        KafkaPerson oldestUs = buildKafkaPerson(
            "Homer",
            "Simpson",
            Instant.parse("1956-08-29T18:35:24Z"),
            CountryCode.US);

        KafkaPerson youngestUs = buildKafkaPerson(
            "Bart",
            "Simpson",
            Instant.parse("1994-11-09T08:08:50Z"),
            CountryCode.US);

        KafkaPerson youngestBe = buildKafkaPerson(
            "Milhouse",
            "Van Houten",
            Instant.parse("1996-02-02T04:58:01Z"),
            CountryCode.BE);

        KafkaPerson oldestBe = buildKafkaPerson(
            "Kirk",
            "Van Houten",
            Instant.parse("1976-05-26T04:52:06Z"),
            CountryCode.BE);

        inputTopic.pipeInput("1", oldestUs);
        inputTopic.pipeInput("2", youngestUs);
        inputTopic.pipeInput("3", youngestBe);
        inputTopic.pipeInput("4", oldestBe);

        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();

        assertEquals(KeyValue.pair(CountryCode.US.toString(), oldestUs), results.get(0));
        assertEquals(KeyValue.pair(CountryCode.US.toString(), oldestUs), results.get(1));
        assertEquals(KeyValue.pair(CountryCode.BE.toString(), youngestBe), results.get(2));
        assertEquals(KeyValue.pair(CountryCode.BE.toString(), oldestBe), results.get(3));

        KeyValueStore<String, KafkaPerson> stateStore = testDriver.getKeyValueStore(PERSON_REDUCE_STORE);

        assertEquals(oldestUs, stateStore.get(CountryCode.US.toString()));
        assertEquals(oldestBe, stateStore.get(CountryCode.BE.toString()));
    }

    private KafkaPerson buildKafkaPerson(String firstName,
                                         String lastName,
                                         Instant birthDate,
                                         CountryCode nationality) {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName(firstName)
            .setLastName(lastName)
            .setBirthDate(birthDate)
            .setNationality(nationality)
            .build();
    }
}
