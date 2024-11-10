package io.github.loicgreffier.streams.aggregate.sliding.window.app;


import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.StateStore.PERSON_AGGREGATE_SLIDING_WINDOW_STORE;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.GROUP_PERSON_BY_LAST_NAME_TOPIC;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.PERSON_AGGREGATE_SLIDING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.sliding.window.app.aggregator.FirstNameByLastNameAggregator;
import io.github.loicgreffier.streams.aggregate.sliding.window.serdes.SerdesUtils;
import java.time.Duration;
import java.util.HashMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.state.WindowStore;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic, selects the key as the last name of the person, groups by key
     * and aggregates the first names by last name in 5-minute tumbling windows with 1-minute grace period.
     * A new key is generated with the window start and end time.
     * The result is written to the PERSON_AGGREGATE_SLIDING_WINDOW_TOPIC topic.
     *
     * <p>
     * {@link org.apache.kafka.streams.kstream.SlidingWindows} are aligned to the records timestamp.
     * They are created each time a record is processed and when a record becomes older than the window size.
     * They are bounded such as:
     * <ul>
     *     <li>[timestamp - window size, timestamp]</li>
     *     <li>[timestamp + 1ms, timestamp + window size + 1ms]</li>
     * </ul>
     * They are looking backward in time.
     * Two records are in the same window if the difference between their timestamps is less than the window size.
     * </p>
     *
     * @see org.apache.kafka.streams.kstream.internals.KStreamSlidingWindowAggregate
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .selectKey((key, person) -> person.getLastName())
            .groupByKey(Grouped.with(GROUP_PERSON_BY_LAST_NAME_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
            .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)))
            .aggregate(
                () -> new KafkaPersonGroup(new HashMap<>()),
                new FirstNameByLastNameAggregator(),
                Materialized
                    .<String, KafkaPersonGroup, WindowStore<Bytes, byte[]>>as(PERSON_AGGREGATE_SLIDING_WINDOW_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SerdesUtils.getValueSerdes())
            )
            .toStream()
            .selectKey((key, groupedPersons) ->
                key.key() + "@" + key.window().startTime() + "->" + key.window().endTime())
            .to(PERSON_AGGREGATE_SLIDING_WINDOW_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
