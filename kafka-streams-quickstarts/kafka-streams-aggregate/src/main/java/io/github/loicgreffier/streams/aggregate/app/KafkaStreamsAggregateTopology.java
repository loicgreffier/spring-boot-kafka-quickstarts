package io.github.loicgreffier.streams.aggregate.app;

import io.github.loicgreffier.streams.aggregate.constants.StateStore;
import io.github.loicgreffier.streams.aggregate.constants.Topic;
import io.github.loicgreffier.streams.aggregate.serdes.CustomSerdes;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.app.aggregator.FirstNameByLastNameAggregator;
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
                .aggregate(() -> new KafkaPersonGroup(new HashMap<>()), new FirstNameByLastNameAggregator(), Named.as("AGGREGATE_PERSON_TOPIC"), Materialized.<String, KafkaPersonGroup, KeyValueStore<Bytes, byte[]>>as(StateStore.PERSON_AGGREGATE_STATE_STORE.toString())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(CustomSerdes.getValueSerdes()))
                .toStream()
                .to(Topic.PERSON_AGGREGATE_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
