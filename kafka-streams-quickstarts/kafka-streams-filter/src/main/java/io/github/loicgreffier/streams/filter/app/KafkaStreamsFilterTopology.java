package io.github.loicgreffier.streams.filter.app;

import io.github.loicgreffier.streams.filter.constants.Topic;
import io.github.loicgreffier.streams.filter.serdes.CustomSerdes;
import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class KafkaStreamsFilterTopology {
    private KafkaStreamsFilterTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .filter((key, person) -> person.getLastName().startsWith("A"))
                .filterNot((key, person) -> !person.getFirstName().startsWith("A"))
                .to(Topic.PERSON_FILTER_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
