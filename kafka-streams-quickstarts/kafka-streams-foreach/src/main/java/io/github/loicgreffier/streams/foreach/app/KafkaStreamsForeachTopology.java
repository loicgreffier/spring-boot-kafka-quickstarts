package io.github.loicgreffier.streams.foreach.app;

import io.github.loicgreffier.streams.foreach.constants.Topic;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.foreach.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;

@Slf4j
public class KafkaStreamsForeachTopology {
    private KafkaStreamsForeachTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .foreach((key, person) -> log.info("Received key = {}, value = {}", key, person));
    }
}
