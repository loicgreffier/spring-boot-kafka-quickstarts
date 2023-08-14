package io.github.loicgreffier.streams.left.join.stream.table.app;

import static io.github.loicgreffier.streams.left.join.stream.table.constants.StateStore.COUNTRY_STATE_STORE;
import static io.github.loicgreffier.streams.left.join.stream.table.constants.Topic.COUNTRY_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.table.constants.Topic.PERSON_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.table.constants.Topic.PERSON_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.table.constants.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinPersonCountry;
import io.github.loicgreffier.avro.KafkaPerson;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the {@link
     * io.github.loicgreffier.streams.left.join.stream.table.constants.Topic#PERSON_TOPIC} topic
     * and the {@link
     * io.github.loicgreffier.streams.left.join.stream.table.constants.Topic#COUNTRY_TOPIC}
     * topic as a table. The stream is joined to the table by nationality with a left join
     * which means a person can exist without a country (a value of null is returned for the
     * country if there is no match). The result is written to the {@link
     * io.github.loicgreffier.streams.left.join.stream.table.constants.Topic#PERSON_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC}
     * topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        KTable<String, KafkaCountry> countryTable = streamsBuilder
            .table(COUNTRY_TOPIC, Materialized.as(COUNTRY_STATE_STORE));

        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .selectKey((key, person) -> person.getNationality().toString())
            .leftJoin(countryTable,
                (key, person, country) -> {
                    if (country != null) {
                        log.info("Joined {} {} {} to country {} by code {}", person.getId(),
                            person.getFirstName(), person.getLastName(),
                            country.getName(), key);
                    } else {
                        log.info("No matching country for {} {} {} with code {}", person.getId(),
                            person.getFirstName(), person.getLastName(), key);
                    }

                    return KafkaJoinPersonCountry.newBuilder()
                        .setPerson(person)
                        .setCountry(country)
                        .build();
                },
                Joined.as(PERSON_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC))
            .to(PERSON_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC);
    }
}
