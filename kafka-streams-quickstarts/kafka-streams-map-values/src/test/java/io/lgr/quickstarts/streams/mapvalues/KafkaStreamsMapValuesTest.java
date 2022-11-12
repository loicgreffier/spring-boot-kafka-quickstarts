package io.lgr.quickstarts.streams.mapvalues;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.lgr.quickstarts.avro.CountryCode;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.mapvalues.app.KafkaStreamsMapValuesTopology;
import io.lgr.quickstarts.streams.mapvalues.constants.Topic;
import io.lgr.quickstarts.streams.mapvalues.serdes.CustomSerdes;
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

class KafkaStreamsMapValuesTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPerson> outputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-map-values-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" +  getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsMapValuesTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.ofEpochMilli(1577836800000L));

        inputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        outputTopic = testDriver.createOutputTopic(Topic.PERSON_MAP_VALUES_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(getClass().getName());
    }

    @Test
    void shouldUpperCase() {
        inputTopic.pipeInput("1", buildKafkaPersonValue());
        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo("1");
        assertThat(results.get(0).value.getId()).isEqualTo(1L);
        assertThat(results.get(0).value.getFirstName()).isEqualTo("FIRST NAME");
        assertThat(results.get(0).value.getLastName()).isEqualTo("LAST NAME");
    }

    private KafkaPerson buildKafkaPersonValue() {
        return KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("First name")
                .setLastName("Last name")
                .setBirthDate(Instant.now())
                .setNationality(CountryCode.FR)
                .build();
    }
}
