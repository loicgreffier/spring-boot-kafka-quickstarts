package io.github.loicgreffier.streams.reduce.app;

import static io.github.loicgreffier.streams.reduce.constant.StateStore.PERSON_REDUCE_STORE;
import static io.github.loicgreffier.streams.reduce.constant.Topic.GROUP_PERSON_BY_NATIONALITY_TOPIC;
import static io.github.loicgreffier.streams.reduce.constant.Topic.PERSON_REDUCE_TOPIC;
import static io.github.loicgreffier.streams.reduce.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.reduce.app.reducer.MaxAgeReducer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic, groups by nationality and reduces the stream to the person with
     * the max age.
     * The result is written to the PERSON_REDUCE_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .groupBy((key, person) -> person.getNationality().toString(),
                Grouped.as(GROUP_PERSON_BY_NATIONALITY_TOPIC))
            .reduce(new MaxAgeReducer(), Materialized.as(PERSON_REDUCE_STORE))
            .toStream()
            .to(PERSON_REDUCE_TOPIC);
    }
}
