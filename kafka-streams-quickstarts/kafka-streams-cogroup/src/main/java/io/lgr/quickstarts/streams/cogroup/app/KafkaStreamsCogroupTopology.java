package io.lgr.quickstarts.streams.cogroup.app;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.avro.KafkaPersonGroup;
import io.lgr.quickstarts.streams.cogroup.app.aggregator.FirstNameByLastNameAggregator;
import io.lgr.quickstarts.streams.cogroup.constants.StateStore;
import io.lgr.quickstarts.streams.cogroup.constants.Topic;
import io.lgr.quickstarts.streams.cogroup.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;

@Slf4j
public class KafkaStreamsCogroupTopology {
    private KafkaStreamsCogroupTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        final FirstNameByLastNameAggregator aggregator = new FirstNameByLastNameAggregator();

        final KGroupedStream<String, KafkaPerson> groupedStreamOne = streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .groupBy((key, person) -> person.getLastName(),
                        Grouped.with("GROUP_BY_" + Topic.PERSON_TOPIC, Serdes.String(), CustomSerdes.getValueSerdes()));

        final KGroupedStream<String, KafkaPerson> groupedStreamTwo = streamsBuilder
                .stream(Topic.PERSON_TOPIC_TWO.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .groupBy((key, person) -> person.getLastName(),
                        Grouped.with("GROUP_BY_" + Topic.PERSON_TOPIC_TWO, Serdes.String(), CustomSerdes.getValueSerdes()));

        groupedStreamOne
                .cogroup(aggregator)
                .cogroup(groupedStreamTwo, aggregator)
                .aggregate(() -> new KafkaPersonGroup(new HashMap<>()), Named.as("AGGREGATE_PERSON_TOPICS"),
                        Materialized.<String, KafkaPersonGroup, KeyValueStore<Bytes, byte[]>>as(StateStore.PERSON_COGROUP_AGGREGATE_STATE_STORE.toString())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(CustomSerdes.getValueSerdes()))
                .toStream()
                .to(Topic.PERSON_COGROUP_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
