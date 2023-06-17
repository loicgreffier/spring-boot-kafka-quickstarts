package io.github.loicgreffier.streams.count.app;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import static io.github.loicgreffier.streams.count.constants.StateStore.PERSON_COUNT_STATE_STORE;
import static io.github.loicgreffier.streams.count.constants.Topic.*;

@Slf4j
public class KafkaStreamsCountTopology {
    private KafkaStreamsCountTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .groupBy((key, person) -> person.getNationality().toString(), Grouped.as(GROUP_PERSON_BY_NATIONALITY_TOPIC))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(PERSON_COUNT_STATE_STORE)
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .to(PERSON_COUNT_TOPIC, Produced.valueSerde(Serdes.Long()));
    }
}
