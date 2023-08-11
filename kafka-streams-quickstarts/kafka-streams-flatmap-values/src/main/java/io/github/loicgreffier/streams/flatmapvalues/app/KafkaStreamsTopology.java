package io.github.loicgreffier.streams.flatmapvalues.app;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;

import static io.github.loicgreffier.streams.flatmapvalues.constants.Topic.PERSON_FLATMAP_VALUES_TOPIC;
import static io.github.loicgreffier.streams.flatmapvalues.constants.Topic.PERSON_TOPIC;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .flatMapValues(person -> Arrays.asList(person.getFirstName(), person.getLastName()))
                .to(PERSON_FLATMAP_VALUES_TOPIC, Produced.valueSerde(Serdes.String()));
    }
}
