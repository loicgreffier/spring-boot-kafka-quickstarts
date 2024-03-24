package io.github.loicgreffier.streams.flatmapvalues.app;

import static io.github.loicgreffier.streams.flatmapvalues.constant.Topic.PERSON_FLATMAP_VALUES_TOPIC;
import static io.github.loicgreffier.streams.flatmapvalues.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import java.util.Arrays;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Produced;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the PERSON_TOPIC topic,
     * maps the value to a list of strings containing the first name and the last name.
     * Write the result to the PERSON_FLATMAP_VALUES_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .flatMapValues(person -> Arrays.asList(person.getFirstName(), person.getLastName()))
            .to(PERSON_FLATMAP_VALUES_TOPIC, Produced.valueSerde(Serdes.String()));
    }
}
