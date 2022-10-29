package io.lgr.quickstarts.streams.reduce.app;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.reduce.app.reducer.MaxAgeReducer;
import io.lgr.quickstarts.streams.reduce.constants.StateStore;
import io.lgr.quickstarts.streams.reduce.constants.Topic;
import io.lgr.quickstarts.streams.reduce.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class KafkaStreamsReduceTopology {
    private KafkaStreamsReduceTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .groupBy((key, person) -> person.getNationality().toString(),
                        Grouped.with("GROUP_BY", Serdes.String(), CustomSerdes.getValueSerdes()))
                .reduce(new MaxAgeReducer(), Named.as("REDUCE"), Materialized.<String, KafkaPerson, KeyValueStore<Bytes, byte[]>>as(StateStore.PERSON_REDUCE_STATE_STORE.toString())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(CustomSerdes.getValueSerdes()))
                .toStream()
                .to(Topic.PERSON_REDUCE_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
