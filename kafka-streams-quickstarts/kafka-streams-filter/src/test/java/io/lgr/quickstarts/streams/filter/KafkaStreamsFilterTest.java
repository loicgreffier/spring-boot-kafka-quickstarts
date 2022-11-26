package io.lgr.quickstarts.streams.filter;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.filter.app.KafkaStreamsFilterTopology;
import io.lgr.quickstarts.streams.filter.constants.Topic;
import io.lgr.quickstarts.streams.filter.serdes.CustomSerdes;
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
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsFilterTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPerson> outputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-filter-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" +  getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsFilterTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        inputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        outputTopic = testDriver.createOutputTopic(Topic.PERSON_FILTER_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(getClass().getName());
    }

    @Test
    void shouldFilterBadLastName() {
        KeyValue<String, KafkaPerson> person = KeyValue.pair("1", buildKafkaPersonValue("First name", "Last name"));
        inputTopic.pipeKeyValueList(Collections.singletonList(person));

        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();

        assertThat(results).isEmpty();
    }

    @Test
    void shouldFilterBadFirstName() {
        KeyValue<String, KafkaPerson> person = KeyValue.pair("1", buildKafkaPersonValue("Diego", "Abbott"));
        inputTopic.pipeKeyValueList(Collections.singletonList(person));

        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();

        assertThat(results).isEmpty();
    }

    @Test
    void shouldNotFilter() {
        KeyValue<String, KafkaPerson> person = KeyValue.pair("1", buildKafkaPersonValue("Akan", "Abbott"));
        inputTopic.pipeKeyValueList(Collections.singletonList(person));

        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isEqualTo(person);
    }

    private KafkaPerson buildKafkaPersonValue(String firstName, String lastName) {
        return KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName(firstName)
                .setLastName(lastName)
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build();
    }
}
