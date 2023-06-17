package io.github.loicgreffier.streams.map.app;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;

import static io.github.loicgreffier.streams.map.constants.Topic.PERSON_MAP_TOPIC;
import static io.github.loicgreffier.streams.map.constants.Topic.PERSON_TOPIC;

@Slf4j
public class KafkaStreamsMapTopology {
    private KafkaStreamsMapTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .map((key, person) -> {
                    person.setFirstName(person.getFirstName().toUpperCase());
                    person.setLastName(person.getLastName().toUpperCase());
                    return KeyValue.pair(person.getLastName(), person);
                })
                .to(PERSON_MAP_TOPIC);
    }
}
