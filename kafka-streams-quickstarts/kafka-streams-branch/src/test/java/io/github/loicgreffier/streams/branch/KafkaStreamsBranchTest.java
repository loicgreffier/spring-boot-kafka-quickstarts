package io.github.loicgreffier.streams.branch;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.github.loicgreffier.streams.branch.app.KafkaStreamsBranchTopology;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.branch.constants.Topic;
import io.github.loicgreffier.streams.branch.serdes.CustomSerdes;
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

class KafkaStreamsBranchTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, KafkaPerson> outputTopicA;
    private TestOutputTopic<String, KafkaPerson> outputTopicB;
    private TestOutputTopic<String, KafkaPerson> outputTopicDefault;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-branch-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" +  getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + getClass().getName());
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsBranchTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.parse("2000-01-01T01:00:00.00Z"));

        inputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        outputTopicA = testDriver.createOutputTopic(Topic.PERSON_BRANCH_A_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());

        outputTopicB = testDriver.createOutputTopic(Topic.PERSON_BRANCH_B_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());

        outputTopicDefault = testDriver.createOutputTopic(Topic.PERSON_BRANCH_DEFAULT_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope("mock://" + getClass().getName());
    }

    @Test
    void shouldBranchToTopicA() {
        KeyValue<String, KafkaPerson> person = KeyValue.pair("1", buildKafkaPersonValue("Calder", "Acosta"));
        inputTopic.pipeKeyValueList(Collections.singletonList(person));

        List<KeyValue<String, KafkaPerson>> results = outputTopicA.readKeyValuesToList();

        KeyValue<String, KafkaPerson> expected = KeyValue.pair("1", buildKafkaPersonValue("CALDER", "ACOSTA"));

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isEqualTo(expected);
    }

    @Test
    void shouldBranchToTopicB() {
        KeyValue<String, KafkaPerson> person = KeyValue.pair("1", buildKafkaPersonValue("Abir", "Barron"));
        inputTopic.pipeKeyValueList(Collections.singletonList(person));

        List<KeyValue<String, KafkaPerson>> results = outputTopicB.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).isEqualTo(person);
    }

    @Test
    void shouldBranchToDefaultTopic() {
        KeyValue<String, KafkaPerson> person = KeyValue.pair("1", buildKafkaPersonValue("Mathew", "Jennings"));
        inputTopic.pipeKeyValueList(Collections.singletonList(person));

        List<KeyValue<String, KafkaPerson>> results = outputTopicDefault.readKeyValuesToList();

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
