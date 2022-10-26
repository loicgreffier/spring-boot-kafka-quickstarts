package io.lgr.quickstarts.streams.merge;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.lgr.quickstarts.avro.CountryCode;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.merge.app.KafkaStreamsMergeTopology;
import io.lgr.quickstarts.streams.merge.constants.Topic;
import io.lgr.quickstarts.streams.merge.serdes.CustomSerdes;
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

class KafkaStreamsMergeTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopicOne;
    private TestInputTopic<String, KafkaPerson> inputTopicTwo;
    private TestOutputTopic<String, KafkaPerson> outputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-merge-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://" +  getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsMergeTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.ofEpochMilli(1577836800000L));

        inputTopicOne = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        inputTopicTwo = testDriver.createInputTopic(Topic.PERSON_TOPIC_TWO.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        outputTopic = testDriver.createOutputTopic(Topic.PERSON_MERGE_TOPIC.toString(), new StringDeserializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().deserializer());
    }

    @AfterEach
    void tearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(getClass().getName());
    }

    @Test
    void testMerge() {
        inputTopicOne.pipeInput("1", buildKafkaPersonValue(1L, "Aaran", "Abbott"));
        inputTopicTwo.pipeInput("2", buildKafkaPersonValue(2L, "Brendan", "Tillman"));
        List<KeyValue<String, KafkaPerson>> results = outputTopic.readKeyValuesToList();

        assertThat(results.get(0).key).isEqualTo("1");
        assertThat(results.get(0).value.getId()).isEqualTo(1L);
        assertThat(results.get(0).value.getFirstName()).isEqualTo("Aaran");
        assertThat(results.get(0).value.getLastName()).isEqualTo("Abbott");

        assertThat(results.get(1).key).isEqualTo("2");
        assertThat(results.get(1).value.getId()).isEqualTo(2L);
        assertThat(results.get(1).value.getFirstName()).isEqualTo("Brendan");
        assertThat(results.get(1).value.getLastName()).isEqualTo("Tillman");
    }

    private KafkaPerson buildKafkaPersonValue(Long id, String firstName, String lastName) {
        return KafkaPerson.newBuilder()
                .setId(id)
                .setFirstName(firstName)
                .setLastName(lastName)
                .setBirthDate(Instant.now())
                .setNationality(CountryCode.FR)
                .build();
    }
}
