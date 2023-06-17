package io.github.loicgreffier.streams.filter.app;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;

import static io.github.loicgreffier.streams.filter.constants.Topic.PERSON_FILTER_TOPIC;
import static io.github.loicgreffier.streams.filter.constants.Topic.PERSON_TOPIC;

@Slf4j
public class KafkaStreamsFilterTopology {
    private KafkaStreamsFilterTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .filter((key, person) -> person.getLastName().startsWith("A"))
                .filterNot((key, person) -> !person.getFirstName().startsWith("A"))
                .to(PERSON_FILTER_TOPIC);
    }
}
