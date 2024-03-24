package io.github.loicgreffier.streams.left.join.stream.global.table.app;

import static io.github.loicgreffier.streams.left.join.stream.global.table.constant.StateStore.COUNTRY_STATE_STORE;
import static io.github.loicgreffier.streams.left.join.stream.global.table.constant.Topic.COUNTRY_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.global.table.constant.Topic.PERSON_COUNTRY_LEFT_JOIN_STREAM_GLOBAL_TABLE_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.global.table.constant.Topic.PERSON_TOPIC;

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
     * Builds the Kafka Streams topology. The topology reads from the PERSON_TOPIC
     * topic and the COUNTRY_TOPIC topic as a global table. The stream is joined to
     * the global table by nationality with a left join which means a person can exist
     * without a country (a value of null is returned for the country if there is no match).
     * The result is written to the PERSON_COUNTRY_LEFT_JOIN_STREAM_GLOBAL_TABLE_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        GlobalKTable<String, KafkaCountry> countryGlobalTable = streamsBuilder
            .globalTable(COUNTRY_TOPIC, Materialized.as(COUNTRY_STATE_STORE));

        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .leftJoin(countryGlobalTable,
                (key, person) -> person.getNationality().toString(),
                (person, country) -> {
                    if (country != null) {
                        log.info("Joined {} {} {} to country {} by code {}", person.getId(),
                            person.getFirstName(), person.getLastName(),
                            country.getName(), country.getCode());
                    } else {
                        log.info("No matching country for {} {} {} with code {}", person.getId(),
                            person.getFirstName(), person.getLastName(), person.getNationality());
                    }

                    return KafkaJoinPersonCountry.newBuilder()
                        .setPerson(person)
                        .setCountry(country)
                        .build();
                })
            .to(PERSON_COUNTRY_LEFT_JOIN_STREAM_GLOBAL_TABLE_TOPIC);
    }
}
