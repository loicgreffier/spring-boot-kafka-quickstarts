package io.github.loicgreffier.streams.processvalues.app;

import static io.github.loicgreffier.streams.processvalues.constant.Topic.PERSON_PROCESS_VALUES_TOPIC;
import static io.github.loicgreffier.streams.processvalues.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.processvalues.app.processor.PersonMetadataFixedKeyProcessor;
import io.github.loicgreffier.streams.processvalues.serdes.SerdesUtils;
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
     * The topology reads from the PERSON_TOPIC topic and processes the records with
     * the {@link PersonMetadataFixedKeyProcessor} processor.
     * The result is written to the PERSON_PROCESS_VALUES_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .processValues(PersonMetadataFixedKeyProcessor::new)
            .to(PERSON_PROCESS_VALUES_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
