package io.github.loicgreffier.streams.left.join.stream.global.table.app;

import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinPersonCountry;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.left.join.stream.global.table.constants.StateStore;
import io.github.loicgreffier.streams.left.join.stream.global.table.constants.Topic;
import io.github.loicgreffier.streams.left.join.stream.global.table.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class KafkaStreamsLeftJoinStreamGlobalTableTopology {
    private KafkaStreamsLeftJoinStreamGlobalTableTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        GlobalKTable<String, KafkaCountry> countryGlobalTable = streamsBuilder
                .globalTable(Topic.COUNTRY_TOPIC.toString(),
                        Consumed.with(Serdes.String(), CustomSerdes.getValueSerdes()),
                        Materialized.<String, KafkaCountry, KeyValueStore<Bytes, byte[]>>as(StateStore.COUNTRY_GLOBAL_TABLE_LEFT_JOIN_STREAM_GLOBAL_TABLE_STATE_STORE.toString())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(CustomSerdes.getValueSerdes()));

        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
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
                .to(Topic.PERSON_COUNTRY_LEFT_JOIN_STREAM_GLOBAL_TABLE_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
