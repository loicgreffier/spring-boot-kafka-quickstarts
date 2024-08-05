package io.github.loicgreffier.streams.average.app;

import static io.github.loicgreffier.streams.average.constant.StateStore.PERSON_AVERAGE_STATE_STORE;
import static io.github.loicgreffier.streams.average.constant.Topic.GROUP_PERSON_BY_NATIONALITY_TOPIC;
import static io.github.loicgreffier.streams.average.constant.Topic.PERSON_AVERAGE_TOPIC;
import static io.github.loicgreffier.streams.average.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaAverageAge;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.average.app.aggregator.AgeAggregator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the PERSON_TOPIC topic,
     * groups by nationality and aggregates the age sum and count.
     * Then, the average age is computed. The result is written to the PERSON_AVERAGE_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .groupBy((key, person) -> person.getNationality().toString(),
                Grouped.as(GROUP_PERSON_BY_NATIONALITY_TOPIC))
            .aggregate(() ->
                new KafkaAverageAge(0L, 0L),
                new AgeAggregator(),
                Materialized.as(PERSON_AVERAGE_STATE_STORE))
            .mapValues(value -> value.getAgeSum() / value.getCount())
            .toStream()
            .to(PERSON_AVERAGE_TOPIC, Produced.valueSerde(Serdes.Long()));
    }
}
