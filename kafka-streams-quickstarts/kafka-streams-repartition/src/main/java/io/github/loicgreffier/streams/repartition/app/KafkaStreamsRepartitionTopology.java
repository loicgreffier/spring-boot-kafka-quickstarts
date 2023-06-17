package io.github.loicgreffier.streams.repartition.app;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Repartitioned;

import static io.github.loicgreffier.streams.repartition.constants.Topic.PERSON_TOPIC;

@Slf4j
public class KafkaStreamsRepartitionTopology {
    private KafkaStreamsRepartitionTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .repartition(Repartitioned.as(PERSON_TOPIC).withNumberOfPartitions(3));
    }
}
