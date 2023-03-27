package io.github.loicgreffier.streams.map.app;

import io.github.loicgreffier.streams.map.constants.Topic;
import io.github.loicgreffier.streams.map.serdes.CustomSerdes;
import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class KafkaStreamsMapTopology {
    private KafkaStreamsMapTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person), Named.as("test-peek"))
                .map((key, person) -> {
                    person.setFirstName(person.getFirstName().toUpperCase());
                    person.setLastName(person.getLastName().toUpperCase());
                    return KeyValue.pair(person.getLastName(), person);
                }, Named.as("test-map"))
                .to(Topic.PERSON_MAP_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
