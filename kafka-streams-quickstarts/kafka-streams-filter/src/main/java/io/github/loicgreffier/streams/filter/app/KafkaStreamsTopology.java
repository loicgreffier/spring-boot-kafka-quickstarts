package io.github.loicgreffier.streams.filter.app;

import static io.github.loicgreffier.streams.filter.constant.Topic.PERSON_FILTER_TOPIC;
import static io.github.loicgreffier.streams.filter.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.filter.serdes.SerdesUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
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
     * The topology reads from the PERSON_TOPIC topic, filters by last name starting with "S"
     * and first name starting with "H".
     * The result is written to the PERSON_FILTER_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .filter((key, person) -> person.getLastName().startsWith("S"))
            .filterNot((key, person) -> !person.getFirstName().startsWith("H"))
            .to(PERSON_FILTER_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
