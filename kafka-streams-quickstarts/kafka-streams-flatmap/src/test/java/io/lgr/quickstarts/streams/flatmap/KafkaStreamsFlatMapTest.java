package io.lgr.quickstarts.streams.flatmap;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.flatmap.app.KafkaStreamsFlatMapTopology;
import io.lgr.quickstarts.streams.flatmap.constants.Topic;
import io.lgr.quickstarts.streams.flatmap.serdes.CustomSerdes;
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

class KafkaStreamsFlatMapTest {
    private final static String STATE_DIR = "/tmp/kafka-streams-quickstarts-test";
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, KafkaPerson> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @BeforeEach
    void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-map-values-test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock://");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        Map<String, String> serdesProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://");
        CustomSerdes.setSerdesConfig(serdesProperties);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsFlatMapTopology.topology(streamsBuilder);
        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, Instant.ofEpochMilli(1577836800000L));

        inputTopic = testDriver.createInputTopic(Topic.PERSON_TOPIC.toString(), new StringSerializer(),
                CustomSerdes.<KafkaPerson>getValueSerdes().serializer());

        outputTopic = testDriver.createOutputTopic(Topic.PERSON_FLATMAP_TOPIC.toString(), new StringDeserializer(),
                new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
        FileUtils.deleteQuietly(new File(STATE_DIR));
        MockSchemaRegistry.dropScope(this.getClass().getName());
    }

    @Test
    void testFirstNameLastNameFlatMapping() {
        KafkaPerson person = KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("First name")
                .setLastName("Last name")
                .setBirthDate(Instant.now())
                .build();

        inputTopic.pipeInput("1", person);

        List<KeyValue<String, String>> results = outputTopic.readKeyValuesToList();

        assertThat(results).hasSize(2);
        assertThat(results.get(0).key).isEqualTo("FIRST NAME");
        assertThat(results.get(0).value).isEqualTo("First name");
        assertThat(results.get(1).key).isEqualTo("LAST NAME");
        assertThat(results.get(1).value).isEqualTo("Last name");
    }
}
