package io.github.loicgreffier.streams.selectkey.app;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;

import static io.github.loicgreffier.streams.selectkey.constants.Topic.PERSON_SELECT_KEY_TOPIC;
import static io.github.loicgreffier.streams.selectkey.constants.Topic.PERSON_TOPIC;

@Slf4j
public class KafkaStreamsSelectKeyTopology {
    private KafkaStreamsSelectKeyTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName())
                .to(PERSON_SELECT_KEY_TOPIC);
    }
}
