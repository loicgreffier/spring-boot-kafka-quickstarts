package io.github.loicgreffier.streams.join.stream.global.table.app;

import static io.github.loicgreffier.streams.join.stream.global.table.constants.StateStore.COUNTRY_STATE_STORE;
import static io.github.loicgreffier.streams.join.stream.global.table.constants.Topic.COUNTRY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.global.table.constants.Topic.PERSON_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC;
import static io.github.loicgreffier.streams.join.stream.global.table.constants.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinPersonCountry;
import io.github.loicgreffier.avro.KafkaPerson;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the PERSON_TOPIC topic
     * and the COUNTRY_TOPIC topic as a global table. The stream is joined to the global table
     * by nationality with an inner join. The result is written to the
     * PERSON_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        GlobalKTable<String, KafkaCountry> countryGlobalTable = streamsBuilder
            .globalTable(COUNTRY_TOPIC, Materialized.as(COUNTRY_STATE_STORE));

        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .join(countryGlobalTable,
                (key, person) -> person.getNationality().toString(),
                (person, country) -> {
                    log.info("Joined {} {} {} to country {} by code {}", person.getId(),
                        person.getFirstName(), person.getLastName(),
                        country.getName(), country.getCode());
                    return KafkaJoinPersonCountry.newBuilder()
                        .setPerson(person)
                        .setCountry(country)
                        .build();
                })
            .to(PERSON_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC);
    }
}
