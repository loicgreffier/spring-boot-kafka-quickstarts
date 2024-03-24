package io.github.loicgreffier.streams.foreach.app;

import static io.github.loicgreffier.streams.foreach.constant.Topic.PERSON_TOPIC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the PERSON_TOPIC topic.
     * For each record, it logs the key and the value.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .stream(PERSON_TOPIC)
            .foreach((key, person) -> log.info("Received key = {}, value = {}", key, person));
    }
}
