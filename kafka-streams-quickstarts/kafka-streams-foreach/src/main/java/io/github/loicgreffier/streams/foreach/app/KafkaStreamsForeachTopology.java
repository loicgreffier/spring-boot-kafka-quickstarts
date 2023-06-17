package io.github.loicgreffier.streams.foreach.app;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;

import static io.github.loicgreffier.streams.foreach.constants.Topic.PERSON_TOPIC;

@Slf4j
public class KafkaStreamsForeachTopology {
    private KafkaStreamsForeachTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(PERSON_TOPIC)
                .foreach((key, person) -> log.info("Received key = {}, value = {}", key, person));
    }
}
