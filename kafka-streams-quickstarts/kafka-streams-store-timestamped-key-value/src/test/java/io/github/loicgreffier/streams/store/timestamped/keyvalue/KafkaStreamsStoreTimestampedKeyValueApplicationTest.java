package io.github.loicgreffier.streams.store.timestamped.keyvalue;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.github.loicgreffier.streams.store.timestamped.keyvalue.constant.StateStore.PERSON_TIMESTAMPED_KEY_VALUE_STORE;
import static io.github.loicgreffier.streams.store.timestamped.keyvalue.constant.StateStore.PERSON_TIMESTAMPED_KEY_VALUE_SUPPLIER_STORE;
import static io.github.loicgreffier.streams.store.timestamped.keyvalue.constant.Topic.PERSON_TOPIC;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.store.timestamped.keyvalue.app.KafkaStreamsTopology;
import io.github.loicgreffier.streams.store.timestamped.keyvalue.serdes.SerdesUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaStreamsStoreTimestampedKeyValueApplicationTest {
    private static final String CLASS_NAME = KafkaStreamsStoreTimestampedKeyValueApplicationTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + CLASS_NAME;
    private static final String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;

    @BeforeEach
    void setUp() {
        // Dummy properties required for test driver
        Properties properties = new Properties();
        properties.setProperty(APPLICATION_ID_CONFIG, "streams-schedule-store-timestamped-key-value-test");
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
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(MOCK_SCHEMA_REGISTRY_URL);
    }

    @Test
    void shouldPutAndGetFromKeyValueStoreRegisteredWithAddStateStore() {
        KafkaPerson homer = buildKafkaPerson("Homer");
        inputTopic.pipeInput(new TestRecord<>("1", homer, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaPerson marge = buildKafkaPerson("Marge");
        inputTopic.pipeInput(new TestRecord<>("2", marge, Instant.parse("2000-01-01T01:00:30Z")));

        KeyValueStore<String, ValueAndTimestamp<KafkaPerson>> timestampedKeyValueStore = testDriver
            .getTimestampedKeyValueStore(PERSON_TIMESTAMPED_KEY_VALUE_STORE);

        assertEquals(homer, timestampedKeyValueStore.get("1").value());
        assertEquals("2000-01-01T01:00:00Z",
            Instant.ofEpochMilli(timestampedKeyValueStore.get("1").timestamp()).toString());
        assertEquals(marge, timestampedKeyValueStore.get("2").value());
        assertEquals("2000-01-01T01:00:30Z",
            Instant.ofEpochMilli(timestampedKeyValueStore.get("2").timestamp()).toString());
    }

    @Test
    void shouldPutAndGetFromKeyValueStoreRegisteredInProcessorSupplier() {
        KafkaPerson homer = buildKafkaPerson("Homer");
        inputTopic.pipeInput(new TestRecord<>("1", homer, Instant.parse("2000-01-01T01:00:00Z")));

        KafkaPerson marge = buildKafkaPerson("Marge");
        inputTopic.pipeInput(new TestRecord<>("2", marge, Instant.parse("2000-01-01T01:00:30Z")));

        KeyValueStore<String, ValueAndTimestamp<KafkaPerson>> timestampedKeyValueStore = testDriver
            .getTimestampedKeyValueStore(PERSON_TIMESTAMPED_KEY_VALUE_SUPPLIER_STORE);

        assertEquals(homer, timestampedKeyValueStore.get("1").value());
        assertEquals("2000-01-01T01:00:00Z",
            Instant.ofEpochMilli(timestampedKeyValueStore.get("1").timestamp()).toString());
        assertEquals(marge, timestampedKeyValueStore.get("2").value());
        assertEquals("2000-01-01T01:00:30Z",
            Instant.ofEpochMilli(timestampedKeyValueStore.get("2").timestamp()).toString());
    }

    private KafkaPerson buildKafkaPerson(String firstName) {
        return KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName(firstName)
            .setLastName("Simpson")
            .setNationality(CountryCode.GB)
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build();
    }
}
