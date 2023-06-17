package io.github.loicgreffier.streams.aggregate.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.app.aggregator.FirstNameByLastNameAggregator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.HashMap;

import static io.github.loicgreffier.streams.aggregate.constants.StateStore.PERSON_AGGREGATE_STATE_STORE;
import static io.github.loicgreffier.streams.aggregate.constants.Topic.*;


@Slf4j
public class KafkaStreamsAggregateTopology {
    private KafkaStreamsAggregateTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName())
                .groupByKey(Grouped.as(GROUP_PERSON_BY_LAST_NAME_TOPIC))
                .aggregate(() -> new KafkaPersonGroup(new HashMap<>()), new FirstNameByLastNameAggregator(), Materialized.as(PERSON_AGGREGATE_STATE_STORE))
                .toStream()
                .to(PERSON_AGGREGATE_TOPIC);
    }
}
