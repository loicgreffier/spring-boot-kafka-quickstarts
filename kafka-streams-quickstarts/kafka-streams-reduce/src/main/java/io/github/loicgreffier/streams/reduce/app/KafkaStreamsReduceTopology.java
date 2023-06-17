package io.github.loicgreffier.streams.reduce.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.reduce.app.reducer.MaxAgeReducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;

import static io.github.loicgreffier.streams.reduce.constants.StateStore.PERSON_REDUCE_STATE_STORE;
import static io.github.loicgreffier.streams.reduce.constants.Topic.*;

@Slf4j
public class KafkaStreamsReduceTopology {
    private KafkaStreamsReduceTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .groupBy((key, person) -> person.getNationality().toString(), Grouped.as(GROUP_PERSON_BY_NATIONALITY_TOPIC))
                .reduce(new MaxAgeReducer(), Materialized.as(PERSON_REDUCE_STATE_STORE))
                .toStream()
                .to(PERSON_REDUCE_TOPIC);
    }
}
