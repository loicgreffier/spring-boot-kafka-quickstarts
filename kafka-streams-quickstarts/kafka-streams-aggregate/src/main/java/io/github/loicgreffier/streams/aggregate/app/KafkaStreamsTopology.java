package io.github.loicgreffier.streams.aggregate.app;

import static io.github.loicgreffier.streams.aggregate.constant.StateStore.PERSON_AGGREGATE_STORE;
import static io.github.loicgreffier.streams.aggregate.constant.Topic.GROUP_PERSON_BY_LAST_NAME_TOPIC;
import static io.github.loicgreffier.streams.aggregate.constant.Topic.PERSON_AGGREGATE_TOPIC;
import static io.github.loicgreffier.streams.aggregate.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.app.aggregator.FirstNameByLastNameAggregator;
import io.github.loicgreffier.streams.aggregate.serdes.SerdesUtils;
import java.util.HashMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic, selects the key as the last name of the person, groups by key
     * and aggregates the first names by last name.
     * The result is written to the PERSON_AGGREGATE_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .selectKey((key, person) -> person.getLastName())
            .groupByKey(Grouped.with(GROUP_PERSON_BY_LAST_NAME_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
            .aggregate(
                () -> new KafkaPersonGroup(new HashMap<>()),
                new FirstNameByLastNameAggregator(),
                Materialized
                    .<String, KafkaPersonGroup, KeyValueStore<Bytes, byte[]>>as(PERSON_AGGREGATE_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SerdesUtils.getValueSerdes())
            )
            .toStream()
            .to(PERSON_AGGREGATE_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
