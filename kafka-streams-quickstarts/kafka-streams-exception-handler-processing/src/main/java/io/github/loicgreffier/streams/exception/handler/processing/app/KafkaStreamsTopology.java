package io.github.loicgreffier.streams.exception.handler.processing.app;


import static io.github.loicgreffier.streams.exception.handler.processing.constant.Topic.PERSON_PROCESSING_EXCEPTION_HANDLER_TOPIC;
import static io.github.loicgreffier.streams.exception.handler.processing.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.exception.handler.processing.app.processor.ErrorProcessor;
import io.github.loicgreffier.streams.exception.handler.processing.serdes.SerdesUtils;
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
     * The topology reads from the PERSON_TOPIC topic.
     * It throws an exception while mapping the value if the birthdate is negative and while processing the value if
     * the first name or the last name is null. Additionally, it schedules a punctuation that periodically throws a
     * processing exception.
     * The result is written to the PERSON_PROCESSING_EXCEPTION_HANDLER_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .mapValues(person -> {
                if (person.getBirthDate().toEpochMilli() < 0) {
                    throw new IllegalArgumentException("Age must be positive");
                }
                return person;
            })
            .processValues(ErrorProcessor::new)
            .to(PERSON_PROCESSING_EXCEPTION_HANDLER_TOPIC,
                Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
