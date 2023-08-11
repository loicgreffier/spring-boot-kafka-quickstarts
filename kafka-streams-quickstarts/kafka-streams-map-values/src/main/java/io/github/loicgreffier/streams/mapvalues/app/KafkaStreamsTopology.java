package io.github.loicgreffier.streams.mapvalues.app;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;

import static io.github.loicgreffier.streams.mapvalues.constants.Topic.PERSON_MAP_VALUES_TOPIC;
import static io.github.loicgreffier.streams.mapvalues.constants.Topic.PERSON_TOPIC;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .mapValues(person -> {
                    person.setFirstName(person.getFirstName().toUpperCase());
                    person.setLastName(person.getLastName().toUpperCase());
                    return person;
                })
                .to(PERSON_MAP_VALUES_TOPIC);
    }
}
