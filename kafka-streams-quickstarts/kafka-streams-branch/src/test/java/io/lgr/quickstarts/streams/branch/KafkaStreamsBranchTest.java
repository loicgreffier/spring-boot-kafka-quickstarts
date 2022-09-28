package io.lgr.quickstarts.streams.branch;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.branch.app.KafkaStreamsBranchTopology;
import io.lgr.quickstarts.streams.branch.constants.Topic;
import io.lgr.quickstarts.streams.branch.serdes.CustomSerdes;
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
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-map-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsBranchTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.ofEpochMilli(1577836800000L));

        inputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getSerdes().serializer());

        outputTopicA = testDriver.createOutputTopic(Topic.PERSON_BRANCH_A_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getSerdes().deserializer());

        outputTopicB = testDriver.createOutputTopic(Topic.PERSON_BRANCH_B_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getSerdes().deserializer());

        outputTopicDefault = testDriver.createOutputTopic(Topic.PERSON_BRANCH_DEFAULT_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getSerdes().deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
        FileUtils.deleteQuietly(new File(STATE_DIR));
        MockSchemaRegistry.dropScope(this.getClass().getName());
    }

    @Test
    void testBranchToTopicA() {
        KafkaPerson person = KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("Calder")
                .setLastName("Acosta")
                .setBirthDate(Instant.now())
                .build();

        inputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaPerson>> results = outputTopicA.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo("1");
        assertThat(results.get(0).value.getId()).isEqualTo(1L);
        assertThat(results.get(0).value.getFirstName()).isEqualTo("CALDER");
        assertThat(results.get(0).value.getLastName()).isEqualTo("ACOSTA");
    }

    @Test
    void testBranchToTopicB() {
        KafkaPerson person = KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("Abir")
                .setLastName("Barron")
                .setBirthDate(Instant.now())
                .build();

        inputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaPerson>> results = outputTopicB.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo("1");
        assertThat(results.get(0).value.getId()).isEqualTo(1L);
        assertThat(results.get(0).value.getFirstName()).isEqualTo("Abir");
        assertThat(results.get(0).value.getLastName()).isEqualTo("Barron");
    }

    @Test
    void testBranchToDefaultTopic() {
        KafkaPerson person = KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("Mathew")
                .setLastName("Jennings")
                .setBirthDate(Instant.now())
                .build();

        inputTopic.pipeInput("1", person);

        List<KeyValue<String, KafkaPerson>> results = outputTopicDefault.readKeyValuesToList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).key).isEqualTo("1");
        assertThat(results.get(0).value.getId()).isEqualTo(1L);
        assertThat(results.get(0).value.getFirstName()).isEqualTo("Mathew");
        assertThat(results.get(0).value.getLastName()).isEqualTo("Jennings");
    }
}
