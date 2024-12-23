package io.github.loicgreffier.streams.average.app;

import static io.github.loicgreffier.streams.average.constant.StateStore.PERSON_AVERAGE_STORE;
import static io.github.loicgreffier.streams.average.constant.Topic.GROUP_PERSON_BY_NATIONALITY_TOPIC;
import static io.github.loicgreffier.streams.average.constant.Topic.PERSON_AVERAGE_TOPIC;
import static io.github.loicgreffier.streams.average.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaAverageAge;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.average.app.aggregator.AgeAggregator;
import io.github.loicgreffier.streams.average.serdes.SerdesUtils;
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
     * The topology reads from the PERSON_TOPIC topic, groups by nationality and aggregates the age sum and count.
     * Then, the average age is computed.
     * The result is written to the PERSON_AVERAGE_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .groupBy((key, person) -> person.getNationality().toString(),
                Grouped.with(GROUP_PERSON_BY_NATIONALITY_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
            .aggregate(
                () -> new KafkaAverageAge(0L, 0L),
                new AgeAggregator(),
                Materialized
                    .<String, KafkaAverageAge, KeyValueStore<Bytes, byte[]>>as(PERSON_AVERAGE_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SerdesUtils.getValueSerdes())
            )
            .mapValues(value -> value.getAgeSum() / value.getCount())
            .toStream()
            .to(PERSON_AVERAGE_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }
}
