package io.github.loicgreffier.streams.reduce.app;

import static io.github.loicgreffier.streams.reduce.constant.StateStore.PERSON_REDUCE_STORE;
import static io.github.loicgreffier.streams.reduce.constant.Topic.GROUP_PERSON_BY_NATIONALITY_TOPIC;
import static io.github.loicgreffier.streams.reduce.constant.Topic.PERSON_REDUCE_TOPIC;
import static io.github.loicgreffier.streams.reduce.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.reduce.app.reducer.MaxAgeReducer;
import io.github.loicgreffier.streams.reduce.serdes.SerdesUtils;
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
     * The topology reads from the PERSON_TOPIC topic, groups by nationality and reduces the stream to the person with
     * the max age.
     * The result is written to the PERSON_REDUCE_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .groupBy((key, person) -> person.getNationality().toString(),
                Grouped.with(GROUP_PERSON_BY_NATIONALITY_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
            .reduce(
                new MaxAgeReducer(),
                Materialized
                    .<String, KafkaPerson, KeyValueStore<Bytes, byte[]>>as(PERSON_REDUCE_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SerdesUtils.getValueSerdes())
            )
            .toStream()
            .to(PERSON_REDUCE_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
