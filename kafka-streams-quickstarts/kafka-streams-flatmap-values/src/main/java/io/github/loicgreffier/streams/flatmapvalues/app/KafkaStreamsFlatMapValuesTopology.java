package io.github.loicgreffier.streams.flatmapvalues.app;

import io.github.loicgreffier.streams.flatmapvalues.constants.Topic;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.flatmapvalues.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;

@Slf4j
public class KafkaStreamsFlatMapValuesTopology {
    private KafkaStreamsFlatMapValuesTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .flatMapValues(person -> Arrays.asList(person.getFirstName(), person.getLastName()))
                .to(Topic.PERSON_FLATMAP_VALUES_TOPIC.toString(), Produced.with(Serdes.String(), Serdes.String()));
    }
}
