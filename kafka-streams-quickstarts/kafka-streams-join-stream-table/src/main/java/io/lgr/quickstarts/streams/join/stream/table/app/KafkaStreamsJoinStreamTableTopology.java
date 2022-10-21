package io.lgr.quickstarts.streams.join.stream.table.app;

import io.lgr.quickstarts.avro.KafkaCountry;
import io.lgr.quickstarts.avro.KafkaJoinPersonCountry;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.join.stream.table.constants.Topic;
import io.lgr.quickstarts.streams.join.stream.table.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class KafkaStreamsJoinStreamTableTopology {
    private KafkaStreamsJoinStreamTableTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getNationality().toString())
                .to(Topic.PERSON_REKEY_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));

        streamsBuilder
                .stream(Topic.COUNTRY_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaCountry>getValueSerdes()))
                .selectKey((key, country) -> country.getCode().toString())
                .to(Topic.COUNTRY_REKEY_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));

        KStream<String, KafkaPerson> personStream = streamsBuilder
                .stream(Topic.PERSON_REKEY_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.getValueSerdes()));

        KTable<String, KafkaCountry> countryTable = streamsBuilder
                .table(Topic.COUNTRY_REKEY_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.getValueSerdes()));

        personStream
                .join(countryTable,
                        (key, person, country) -> {
                            log.info("Joined {} {} by code {}", person.getFirstName(), person.getLastName(), key);
                            return KafkaJoinPersonCountry.newBuilder()
                                    .setPerson(person)
                                    .setCountry(country)
                                    .build();
                        },
                        Joined.with(Serdes.String(), CustomSerdes.getValueSerdes(), CustomSerdes.getValueSerdes(), Topic.JOIN_PERSON_COUNTRY_TOPIC.toString()))
                .to(Topic.JOIN_PERSON_COUNTRY_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
