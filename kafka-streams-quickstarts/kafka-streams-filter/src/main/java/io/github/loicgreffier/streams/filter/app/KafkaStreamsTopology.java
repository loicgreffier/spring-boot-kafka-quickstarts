package io.github.loicgreffier.streams.filter.app;

import static io.github.loicgreffier.streams.filter.constant.Topic.PERSON_FILTER_TOPIC;
import static io.github.loicgreffier.streams.filter.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
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
     * Builds the Kafka Streams topology. The topology reads from the
     * PERSON_TOPIC topic, filters by last name and first name starting with "A".
     * Then, the result is written to the PERSON_FILTER_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .filter((key, person) -> person.getLastName().startsWith("A"))
            .filterNot((key, person) -> !person.getFirstName().startsWith("A"))
            .to(PERSON_FILTER_TOPIC);
    }
}
