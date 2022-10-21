package io.lgr.quickstarts.streams.join.stream.table.app;

import io.lgr.quickstarts.avro.KafkaCountry;
import io.lgr.quickstarts.avro.KafkaJoinPersonCountry;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.join.stream.table.constants.StateStore;
import io.lgr.quickstarts.streams.join.stream.table.constants.Topic;
import io.lgr.quickstarts.streams.join.stream.table.serdes.CustomSerdes;
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
                .stream(Topic.COUNTRY_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaCountry>getValueSerdes()))
                .selectKey((key, country) -> country.getCode().toString())
                .repartition(Repartitioned
                        .<String, KafkaCountry>with(Serdes.String(), CustomSerdes.getValueSerdes())
                        .withName(Topic.COUNTRY_TOPIC.toString()))
                .toTable(Materialized.<String, KafkaCountry, KeyValueStore<Bytes, byte[]>>as(StateStore.COUNTRY_TABLE_STATE_STORE.toString())
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
                        Joined.with(Serdes.String(), CustomSerdes.getValueSerdes(), CustomSerdes.getValueSerdes(), Topic.JOIN_PERSON_COUNTRY_TOPIC.toString()))
                .to(Topic.JOIN_PERSON_COUNTRY_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
