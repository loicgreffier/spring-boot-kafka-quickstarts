package io.github.loicgreffier.streams.aggregate.hopping.window.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.hopping.window.app.aggregator.FirstNameByLastNameAggregator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;

import java.time.Duration;
import java.util.HashMap;

import static io.github.loicgreffier.streams.aggregate.hopping.window.constants.StateStore.PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constants.Topic.*;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName())
                .groupByKey(Grouped.as(GROUP_PERSON_BY_LAST_NAME_TOPIC))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)).advanceBy(Duration.ofMinutes(2)))
                .aggregate(() -> new KafkaPersonGroup(new HashMap<>()), new FirstNameByLastNameAggregator(), Materialized.as(PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE))
                .toStream()
                .selectKey((key, groupedPersons) -> key.key() + "@" + key.window().startTime() + "->" + key.window().endTime())
                .to(PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC);
    }
}
