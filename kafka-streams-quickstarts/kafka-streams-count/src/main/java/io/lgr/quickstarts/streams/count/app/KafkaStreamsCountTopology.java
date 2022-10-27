package io.lgr.quickstarts.streams.count.app;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.count.constants.StateStore;
import io.lgr.quickstarts.streams.count.constants.Topic;
import io.lgr.quickstarts.streams.count.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class KafkaStreamsCountTopology {
    private KafkaStreamsCountTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .groupBy((key, person) -> person.getLastName(),
                        Grouped.with("GROUP_BY", Serdes.String(), CustomSerdes.getValueSerdes()))
                .count(Named.as("COUNT"), Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(StateStore.PERSON_COUNT_STATE_STORE.toString())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .to(Topic.PERSON_COUNT_TOPIC.toString(), Produced.with(Serdes.String(), Serdes.Long()));
    }
}
