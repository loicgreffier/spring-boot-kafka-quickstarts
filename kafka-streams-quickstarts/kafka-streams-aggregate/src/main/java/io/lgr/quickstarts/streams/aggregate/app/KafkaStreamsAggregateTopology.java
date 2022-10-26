package io.lgr.quickstarts.streams.aggregate.app;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.avro.KafkaPersonGroup;
import io.lgr.quickstarts.streams.aggregate.app.aggregator.FirstNameByLastNameAggregator;
import io.lgr.quickstarts.streams.aggregate.constants.StateStore;
import io.lgr.quickstarts.streams.aggregate.constants.Topic;
import io.lgr.quickstarts.streams.aggregate.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;

@Slf4j
public class KafkaStreamsAggregateTopology {
    private KafkaStreamsAggregateTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName())
                .groupByKey(Grouped.with("GROUP_BY_" + Topic.PERSON_TOPIC, Serdes.String(), CustomSerdes.getValueSerdes()))
                .aggregate(() -> new KafkaPersonGroup(new HashMap<>()), new FirstNameByLastNameAggregator(), Named.as("AGGREGATE_PERSON_TOPICS"), Materialized.<String, KafkaPersonGroup, KeyValueStore<Bytes, byte[]>>as(StateStore.PERSON_AGGREGATE_STATE_STORE.toString())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(CustomSerdes.getValueSerdes()))
                .toStream()
                .to(Topic.PERSON_AGGREGATE_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
