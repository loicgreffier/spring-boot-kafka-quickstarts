package io.github.loicgreffier.streams.process.app;

import static io.github.loicgreffier.streams.process.constant.Topic.PERSON_PROCESS_TOPIC;
import static io.github.loicgreffier.streams.process.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.process.app.processor.PersonMetadataProcessor;
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
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic and processes the records with
     * the {@link PersonMetadataProcessor} processor.
     * The result is written to the PERSON_PROCESS_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .process(PersonMetadataProcessor::new)
            .to(PERSON_PROCESS_TOPIC);
    }
}
