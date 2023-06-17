package io.github.loicgreffier.streams.processvalues.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.processvalues.app.processor.PersonMetadataFixedKeyProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;

import static io.github.loicgreffier.streams.processvalues.constants.Topic.PERSON_PROCESS_VALUES_TOPIC;
import static io.github.loicgreffier.streams.processvalues.constants.Topic.PERSON_TOPIC;

@Slf4j
public class KafkaStreamsProcessValuesTopology {
    private KafkaStreamsProcessValuesTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .processValues(PersonMetadataFixedKeyProcessor::new)
                .to(PERSON_PROCESS_VALUES_TOPIC);
    }
}
