package io.github.loicgreffier.streams.merge.app;

import static io.github.loicgreffier.streams.merge.constants.Topic.PERSON_MERGE_TOPIC;
import static io.github.loicgreffier.streams.merge.constants.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.merge.constants.Topic.PERSON_TOPIC_TWO;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the PERSON_TOPIC topic
     * and the PERSON_TOPIC_TWO topic, then merge the two streams into the
     * PERSON_MERGE_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> streamOne = streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person));

        KStream<String, KafkaPerson> streamTwo = streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC_TWO)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person));

        streamOne
            .merge(streamTwo)
            .to(PERSON_MERGE_TOPIC);
    }
}
