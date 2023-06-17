package io.github.loicgreffier.streams.merge.app;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import static io.github.loicgreffier.streams.merge.constants.Topic.*;

@Slf4j
public class KafkaStreamsMergeTopology {
    private KafkaStreamsMergeTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> streamOne = streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person));

        KStream<String, KafkaPerson> streamTwo = streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC_TWO)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person));

        streamOne
                .merge(streamTwo)
                .to(PERSON_MERGE_TOPIC);
    }
}
