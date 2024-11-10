package io.github.loicgreffier.streams.join.stream.globaltable.app;

import static io.github.loicgreffier.streams.join.stream.globaltable.constant.StateStore.COUNTRY_STORE;
import static io.github.loicgreffier.streams.join.stream.globaltable.constant.Topic.COUNTRY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.globaltable.constant.Topic.PERSON_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC;
import static io.github.loicgreffier.streams.join.stream.globaltable.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinPersonCountry;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.join.stream.globaltable.serdes.SerdesUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic and the COUNTRY_TOPIC topic as a global table.
     * The stream is joined to the global table by nationality with an inner join.
     * The result is written to the PERSON_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC topic.
     *
     * <p>
     * An inner join emits an output when both streams have records with the same key.
     * </p>
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        GlobalKTable<String, KafkaCountry> countryGlobalTable = streamsBuilder
            .globalTable(COUNTRY_TOPIC, Materialized
                .<String, KafkaCountry, KeyValueStore<Bytes, byte[]>>as(COUNTRY_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(SerdesUtils.getValueSerdes()));

        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
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
            .to(PERSON_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC,
                Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
