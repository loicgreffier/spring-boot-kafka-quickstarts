package io.github.loicgreffier.streams.aggregate.app;

import static io.github.loicgreffier.streams.aggregate.constants.StateStore.PERSON_AGGREGATE_STATE_STORE;
import static io.github.loicgreffier.streams.aggregate.constants.Topic.GROUP_PERSON_BY_LAST_NAME_TOPIC;
import static io.github.loicgreffier.streams.aggregate.constants.Topic.PERSON_AGGREGATE_TOPIC;
import static io.github.loicgreffier.streams.aggregate.constants.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.app.aggregator.FirstNameByLastNameAggregator;
import java.util.HashMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the
     * {@link io.github.loicgreffier.streams.aggregate.constants.Topic#PERSON_TOPIC} topic,
     * selects the key as the last name of the person, groups by key
     * and aggregates the first names by last name. The result is written to the
     * {@link io.github.loicgreffier.streams.aggregate.constants.Topic#PERSON_AGGREGATE_TOPIC}
     * topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .selectKey((key, person) -> person.getLastName())
            .groupByKey(Grouped.as(GROUP_PERSON_BY_LAST_NAME_TOPIC))
            .aggregate(() -> new KafkaPersonGroup(new HashMap<>()),
                new FirstNameByLastNameAggregator(), Materialized.as(PERSON_AGGREGATE_STATE_STORE))
            .toStream()
            .to(PERSON_AGGREGATE_TOPIC);
    }
}
