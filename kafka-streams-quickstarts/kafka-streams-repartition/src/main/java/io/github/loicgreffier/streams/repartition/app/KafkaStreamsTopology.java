package io.github.loicgreffier.streams.repartition.app;

import static io.github.loicgreffier.streams.repartition.constants.Topic.PERSON_TOPIC;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Repartitioned;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the
     * {@link io.github.loicgreffier.streams.repartition.constants.Topic#PERSON_TOPIC} topic
     * and repartitions the stream with 3 partitions.
     * Then, the result is written to the
     * {@link io.github.loicgreffier.streams.repartition.constants.Topic#PERSON_TOPIC}
     * repartition topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .repartition(Repartitioned.as(PERSON_TOPIC).withNumberOfPartitions(3));
    }
}
