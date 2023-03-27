package io.github.loicgreffier.streams.join.stream.table.app;

import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinPersonCountry;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.join.stream.table.constants.StateStore;
import io.github.loicgreffier.streams.join.stream.table.constants.Topic;
import io.github.loicgreffier.streams.join.stream.table.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class KafkaStreamsJoinStreamTableTopology {
    private KafkaStreamsJoinStreamTableTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        KTable<String, KafkaCountry> countryTable = streamsBuilder
                .table(Topic.COUNTRY_TOPIC.toString(),
                        Consumed.with(Serdes.String(), CustomSerdes.getValueSerdes()),
                        Materialized.<String, KafkaCountry, KeyValueStore<Bytes, byte[]>>as(StateStore.COUNTRY_TABLE_JOIN_STREAM_TABLE_STATE_STORE.toString())
                                .withKeySerde(Serdes.String())
                                .withValueSerde(CustomSerdes.getValueSerdes()));

        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getNationality().toString())
                .join(countryTable,
                        (key, person, country) -> {
                            log.info("Joined {} {} {} to country {} by code {}", person.getId(), person.getFirstName(), person.getLastName(),
                                    country.getName(), key);
                            return KafkaJoinPersonCountry.newBuilder()
                                    .setPerson(person)
                                    .setCountry(country)
                                    .build();
                        },
                        Joined.with(Serdes.String(), CustomSerdes.getValueSerdes(), CustomSerdes.getValueSerdes(), Topic.PERSON_JOIN_STREAM_TABLE_REKEY_TOPIC.toString()))
                .to(Topic.PERSON_COUNTRY_JOIN_STREAM_TABLE_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
