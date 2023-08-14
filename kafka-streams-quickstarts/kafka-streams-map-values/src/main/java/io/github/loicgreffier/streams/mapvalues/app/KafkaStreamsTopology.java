package io.github.loicgreffier.streams.mapvalues.app;

import static io.github.loicgreffier.streams.mapvalues.constants.Topic.PERSON_MAP_VALUES_TOPIC;
import static io.github.loicgreffier.streams.mapvalues.constants.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the PERSON_TOPIC topic,
     * maps the first name and the last name to upper case. The result is written to
     * the PERSON_MAP_VALUES_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .mapValues(person -> {
                person.setFirstName(person.getFirstName().toUpperCase());
                person.setLastName(person.getLastName().toUpperCase());
                return person;
            })
            .to(PERSON_MAP_VALUES_TOPIC);
    }
}
