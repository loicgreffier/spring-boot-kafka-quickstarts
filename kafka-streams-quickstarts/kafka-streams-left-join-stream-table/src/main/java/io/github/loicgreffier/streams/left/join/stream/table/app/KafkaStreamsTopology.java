package io.github.loicgreffier.streams.left.join.stream.table.app;

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

import static io.github.loicgreffier.streams.left.join.stream.table.constants.StateStore.COUNTRY_TABLE_LEFT_JOIN_STREAM_TABLE_STATE_STORE;
import static io.github.loicgreffier.streams.left.join.stream.table.constants.Topic.*;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {
    public static void topology(StreamsBuilder streamsBuilder) {
        KTable<String, KafkaCountry> countryTable = streamsBuilder
                .table(COUNTRY_TOPIC, Materialized.as(COUNTRY_TABLE_LEFT_JOIN_STREAM_TABLE_STATE_STORE));

        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getNationality().toString())
                .leftJoin(countryTable,
                        (key, person, country) -> {
                            if (country != null) {
                                log.info("Joined {} {} {} to country {} by code {}", person.getId(), person.getFirstName(), person.getLastName(),
                                        country.getName(), key);
                            } else {
                                log.info("No matching country for {} {} {} with code {}", person.getId(), person.getFirstName(), person.getLastName(), key);
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
