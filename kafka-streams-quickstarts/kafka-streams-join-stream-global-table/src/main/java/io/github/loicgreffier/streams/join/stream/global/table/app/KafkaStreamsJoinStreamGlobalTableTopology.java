package io.github.loicgreffier.streams.join.stream.global.table.app;

import io.github.loicgreffier.streams.join.stream.global.table.constants.StateStore;
import io.github.loicgreffier.streams.join.stream.global.table.constants.Topic;
import io.github.loicgreffier.streams.join.stream.global.table.serdes.CustomSerdes;
import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinPersonCountry;
import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class KafkaStreamsJoinStreamGlobalTableTopology {
    private KafkaStreamsJoinStreamGlobalTableTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        GlobalKTable<String, KafkaCountry> countryGlobalTable = streamsBuilder
                .globalTable(Topic.COUNTRY_TOPIC.toString(),
                        Consumed.with(Serdes.String(), CustomSerdes.getValueSerdes()),
                        Materialized.<String, KafkaCountry, KeyValueStore<Bytes, byte[]>>as(StateStore.COUNTRY_GLOBAL_TABLE_JOIN_STREAM_GLOBAL_TABLE_STATE_STORE.toString())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(CustomSerdes.getValueSerdes()));

        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .join(countryGlobalTable,
                        (key, person) -> person.getNationality().toString(),
                        (person, country) -> {
                            log.info("Joined {} {} {} to country {} by code {}", person.getId(), person.getFirstName(), person.getLastName(),
                                    country.getName(), country.getCode());
                            return KafkaJoinPersonCountry.newBuilder()
                                    .setPerson(person)
                                    .setCountry(country)
                                    .build();
                        })
                .to(Topic.PERSON_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
