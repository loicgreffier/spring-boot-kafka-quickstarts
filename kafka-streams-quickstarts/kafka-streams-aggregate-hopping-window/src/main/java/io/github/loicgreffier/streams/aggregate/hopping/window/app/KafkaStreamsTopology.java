package io.github.loicgreffier.streams.aggregate.hopping.window.app;

import static io.github.loicgreffier.streams.aggregate.hopping.window.constant.StateStore.PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constant.Topic.GROUP_PERSON_BY_LAST_NAME_TOPIC;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constant.Topic.PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.hopping.window.app.aggregator.FirstNameByLastNameAggregator;
import java.time.Duration;
import java.util.HashMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the PERSON_TOPIC topic,
     * selects the key as the last name of the person, groups by key
     * and aggregates the first names by last name in a 5 minutes hopping window with
     * 1-minute grace period and 2 minutes advance period.
     * A new key is generated with the window start and end time.
     * The result is written to the PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .selectKey((key, person) -> person.getLastName())
            .groupByKey(Grouped.as(GROUP_PERSON_BY_LAST_NAME_TOPIC))
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1))
                .advanceBy(Duration.ofMinutes(2)))
            .aggregate(() -> new KafkaPersonGroup(new HashMap<>()),
                new FirstNameByLastNameAggregator(),
                Materialized.as(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE))
            .toStream()
            .selectKey((key, groupedPersons) -> key.key() + "@" + key.window().startTime() + "->"
                + key.window().endTime())
            .to(PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC);
    }
}
