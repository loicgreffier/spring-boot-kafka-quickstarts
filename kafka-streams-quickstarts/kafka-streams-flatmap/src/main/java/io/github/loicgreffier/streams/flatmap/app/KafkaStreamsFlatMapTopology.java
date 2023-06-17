package io.github.loicgreffier.streams.flatmap.app;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;

import static io.github.loicgreffier.streams.flatmap.constants.Topic.PERSON_FLATMAP_TOPIC;
import static io.github.loicgreffier.streams.flatmap.constants.Topic.PERSON_TOPIC;

@Slf4j
public class KafkaStreamsFlatMapTopology {
    private KafkaStreamsFlatMapTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .flatMap((key, person) ->
                        Arrays.asList(KeyValue.pair(person.getFirstName().toUpperCase(), person.getFirstName()),
                                KeyValue.pair(person.getLastName().toUpperCase(), person.getLastName())))
                .to(PERSON_FLATMAP_TOPIC, Produced.valueSerde(Serdes.String()));
    }
}
