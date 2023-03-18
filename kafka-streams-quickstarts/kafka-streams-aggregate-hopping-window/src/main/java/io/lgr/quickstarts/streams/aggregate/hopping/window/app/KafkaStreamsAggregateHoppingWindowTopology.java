package io.lgr.quickstarts.streams.aggregate.hopping.window.app;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.avro.KafkaPersonGroup;
import io.lgr.quickstarts.streams.aggregate.hopping.window.app.aggregator.FirstNameByLastNameAggregator;
import io.lgr.quickstarts.streams.aggregate.hopping.window.constants.StateStore;
import io.lgr.quickstarts.streams.aggregate.hopping.window.constants.Topic;
import io.lgr.quickstarts.streams.aggregate.hopping.window.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.HashMap;

@Slf4j
public class KafkaStreamsAggregateHoppingWindowTopology {
    private KafkaStreamsAggregateHoppingWindowTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName())
                .groupByKey(Grouped.with("GROUP_BY_" + Topic.PERSON_TOPIC, Serdes.String(), CustomSerdes.getValueSerdes()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)).advanceBy(Duration.ofMinutes(2)))
                .aggregate(() -> new KafkaPersonGroup(new HashMap<>()), new FirstNameByLastNameAggregator(), Named.as("AGGREGATE_HOPPING_WINDOW_PERSON_TOPIC"),
                        Materialized.<String, KafkaPersonGroup, WindowStore<Bytes, byte[]>>as(StateStore.PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE.toString())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(CustomSerdes.getValueSerdes()))
                .toStream()
                .selectKey((key, groupedPersons) -> key.key() + "@" + key.window().startTime() + "->" + key.window().endTime())
                .to(Topic.PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
