package io.github.loicgreffier.streams.flatmap.app;

import static io.github.loicgreffier.streams.flatmap.constants.Topic.PERSON_FLATMAP_TOPIC;
import static io.github.loicgreffier.streams.flatmap.constants.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Produced;


/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the
     * {@link io.github.loicgreffier.streams.flatmap.constants.Topic#PERSON_TOPIC} topic,
     * maps the value to a list of key-value pairs containing the first name and the last name
     * as key and value respectively. Upper case the key and write the result to the
     * {@link io.github.loicgreffier.streams.flatmap.constants.Topic#PERSON_FLATMAP_TOPIC} topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .flatMap((key, person) ->
                Arrays.asList(
                    KeyValue.pair(person.getFirstName().toUpperCase(), person.getFirstName()),
                    KeyValue.pair(person.getLastName().toUpperCase(), person.getLastName())))
            .to(PERSON_FLATMAP_TOPIC, Produced.valueSerde(Serdes.String()));
    }
}
