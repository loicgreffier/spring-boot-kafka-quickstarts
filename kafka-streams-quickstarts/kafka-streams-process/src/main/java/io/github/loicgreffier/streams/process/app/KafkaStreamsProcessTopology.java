package io.github.loicgreffier.streams.process.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.process.app.processor.PersonMetadataProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;

import static io.github.loicgreffier.streams.process.constants.Topic.PERSON_PROCESS_TOPIC;
import static io.github.loicgreffier.streams.process.constants.Topic.PERSON_TOPIC;

@Slf4j
public class KafkaStreamsProcessTopology {
    private KafkaStreamsProcessTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .process(PersonMetadataProcessor::new)
                .to(PERSON_PROCESS_TOPIC);
    }
}
