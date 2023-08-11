package io.github.loicgreffier.streams.cogroup.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.cogroup.app.aggregator.FirstNameByLastNameAggregator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.HashMap;

import static io.github.loicgreffier.streams.cogroup.constants.StateStore.PERSON_COGROUP_AGGREGATE_STATE_STORE;
import static io.github.loicgreffier.streams.cogroup.constants.Topic.*;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamTopology {
    public static void topology(StreamsBuilder streamsBuilder) {
        final FirstNameByLastNameAggregator aggregator = new FirstNameByLastNameAggregator();

        final KGroupedStream<String, KafkaPerson> groupedStreamOne = streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .groupBy((key, person) -> person.getLastName(), Grouped.as(GROUP_PERSON_BY_LAST_NAME_TOPIC));

        final KGroupedStream<String, KafkaPerson> groupedStreamTwo = streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC_TWO)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .groupBy((key, person) -> person.getLastName(), Grouped.as(GROUP_PERSON_BY_LAST_NAME_TOPIC_TWO));

        groupedStreamOne
                .cogroup(aggregator)
                .cogroup(groupedStreamTwo, aggregator)
                .aggregate(() -> new KafkaPersonGroup(new HashMap<>()), Materialized.as(PERSON_COGROUP_AGGREGATE_STATE_STORE))
                .toStream()
                .to(PERSON_COGROUP_TOPIC);
    }
}
