package io.github.loicgreffier.streams.selectkey.app;

import static io.github.loicgreffier.streams.selectkey.constants.Topic.PERSON_SELECT_KEY_TOPIC;
import static io.github.loicgreffier.streams.selectkey.constants.Topic.PERSON_TOPIC;

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
     * Builds the Kafka Streams topology. The topology reads from the
     * {@link io.github.loicgreffier.streams.selectkey.constants.Topic#PERSON_TOPIC} topic
     * and changes the key to the last name.
     * The result is written to the {@link
     * io.github.loicgreffier.streams.selectkey.constants.Topic#PERSON_SELECT_KEY_TOPIC} topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .selectKey((key, person) -> person.getLastName())
            .to(PERSON_SELECT_KEY_TOPIC);
    }
}
