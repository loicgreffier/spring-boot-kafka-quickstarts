package io.github.loicgreffier.streams.flatmap.app;

import io.github.loicgreffier.streams.flatmap.serdes.CustomSerdes;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.flatmap.constants.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;

@Slf4j
public class KafkaStreamsFlatMapTopology {
    private KafkaStreamsFlatMapTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .flatMap((key, person) ->
                        Arrays.asList(KeyValue.pair(person.getFirstName().toUpperCase(), person.getFirstName()),
                                KeyValue.pair(person.getLastName().toUpperCase(), person.getLastName()))
                )
                .to(Topic.PERSON_FLATMAP_TOPIC.toString(), Produced.with(Serdes.String(), Serdes.String()));
    }
}
