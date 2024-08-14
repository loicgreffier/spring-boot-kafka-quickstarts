package io.github.loicgreffier.streams.join.stream.table.app;

import static io.github.loicgreffier.streams.join.stream.table.constant.StateStore.COUNTRY_STATE_STORE;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.COUNTRY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.PERSON_COUNTRY_JOIN_STREAM_TABLE_TOPIC;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.PERSON_JOIN_STREAM_TABLE_REKEY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.table.constant.Topic.PERSON_TOPIC;

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
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic and the COUNTRY_TOPIC topic as a table.
     * The stream is joined to the table by nationality with an inner join.
     * The result is written to the PERSON_COUNTRY_JOIN_STREAM_TABLE_TOPIC topic.
     * <p>
     * An inner join emits an output when both streams have records with the same key.
     * </p>
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
            .join(countryTable,
                (key, person, country) -> {
                    log.info("Joined {} {} {} to country {} by code {}", person.getId(),
                        person.getFirstName(), person.getLastName(),
                        country.getName(), key);
                    return KafkaJoinPersonCountry.newBuilder()
                        .setPerson(person)
                        .setCountry(country)
                        .build();
                },
                Joined.as(PERSON_JOIN_STREAM_TABLE_REKEY_TOPIC))
            .to(PERSON_COUNTRY_JOIN_STREAM_TABLE_TOPIC);
    }
}
