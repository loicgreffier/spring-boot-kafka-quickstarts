package io.github.loicgreffier.streams.left.join.stream.global.table.app;

import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinPersonCountry;
import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;

import static io.github.loicgreffier.streams.left.join.stream.global.table.constants.StateStore.COUNTRY_GLOBAL_TABLE_LEFT_JOIN_STREAM_GLOBAL_TABLE_STATE_STORE;
import static io.github.loicgreffier.streams.left.join.stream.global.table.constants.Topic.*;

@Slf4j
public class KafkaStreamsLeftJoinStreamGlobalTableTopology {
    private KafkaStreamsLeftJoinStreamGlobalTableTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        GlobalKTable<String, KafkaCountry> countryGlobalTable = streamsBuilder
                .globalTable(COUNTRY_TOPIC, Materialized.as(COUNTRY_GLOBAL_TABLE_LEFT_JOIN_STREAM_GLOBAL_TABLE_STATE_STORE));

        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .leftJoin(countryGlobalTable,
                        (key, person) -> person.getNationality().toString(),
                        (person, country) -> {
                            if (country != null) {
                                log.info("Joined {} {} {} to country {} by code {}", person.getId(), person.getFirstName(), person.getLastName(),
                                        country.getName(), country.getCode());
                            } else {
                                log.info("No matching country for {} {} {} with code {}", person.getId(), person.getFirstName(), person.getLastName(), person.getNationality());
                            }

                            return KafkaJoinPersonCountry.newBuilder()
                                    .setPerson(person)
                                    .setCountry(country)
                                    .build();
                        })
                .to(PERSON_COUNTRY_LEFT_JOIN_STREAM_GLOBAL_TABLE_TOPIC);
    }
}
