package io.github.loicgreffier.streams.count.app;

import static io.github.loicgreffier.streams.count.constant.StateStore.PERSON_COUNT_STORE;
import static io.github.loicgreffier.streams.count.constant.Topic.GROUP_PERSON_BY_NATIONALITY_TOPIC;
import static io.github.loicgreffier.streams.count.constant.Topic.PERSON_COUNT_TOPIC;
import static io.github.loicgreffier.streams.count.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.count.serdes.SerdesUtils;
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
     * The topology reads from the PERSON_TOPIC topic, groups by nationality and counts the number of persons.
     * The result is written to the PERSON_COUNT_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .groupBy((key, person) -> person.getNationality().toString(),
                Grouped.with(GROUP_PERSON_BY_NATIONALITY_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
            .count(Materialized
                .<String, Long, KeyValueStore<Bytes, byte[]>>as(PERSON_COUNT_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()))
            .toStream()
            .to(PERSON_COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }
}
