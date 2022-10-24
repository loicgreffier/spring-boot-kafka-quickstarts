package io.lgr.quickstarts.streams.print.app;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.print.constants.Topic;
import io.lgr.quickstarts.streams.print.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;

@Slf4j
public class KafkaStreamsPrintTopology {
    private KafkaStreamsPrintTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .print(Printed.<String, KafkaPerson>toFile("/tmp/kafka-streams-quickstarts/streams-print-output.log")
                        .withKeyValueMapper(KafkaStreamsPrintTopology::toOutput)
                        .withLabel(Topic.PERSON_TOPIC.toString()));

        streamsBuilder
                .stream(Topic.PERSON_TOPIC_TWO.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .print(Printed.<String, KafkaPerson>toSysOut()
                        .withKeyValueMapper(KafkaStreamsPrintTopology::toOutput)
                        .withLabel(Topic.PERSON_TOPIC_TWO.toString()));
    }

    private static String toOutput(String key, KafkaPerson kafkaPerson) {
        return String.format("Received key = %s, value = %s", key, kafkaPerson);
    }
}
