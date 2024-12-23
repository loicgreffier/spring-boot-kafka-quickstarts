package io.github.loicgreffier.streams.flatmap.app;

import static io.github.loicgreffier.streams.flatmap.constant.Topic.PERSON_FLATMAP_TOPIC;
import static io.github.loicgreffier.streams.flatmap.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.flatmap.serdes.SerdesUtils;
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;


/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic, maps the value to a list of key-value pairs containing
     * the first name and the last name as key and value respectively and upper case the key.
     * The result is written to the PERSON_FLATMAP_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .flatMap((key, person) -> Arrays.asList(
                KeyValue.pair(person.getFirstName().toUpperCase(), person.getFirstName()),
                KeyValue.pair(person.getLastName().toUpperCase(), person.getLastName())))
            .to(PERSON_FLATMAP_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }
}
