package io.github.loicgreffier.streams.merge.app;

import io.github.loicgreffier.streams.merge.constants.Topic;
import io.github.loicgreffier.streams.merge.serdes.CustomSerdes;
import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class KafkaStreamsMergeTopology {
    private KafkaStreamsMergeTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        final KStream<String, KafkaPerson> streamOne = streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person));

        final KStream<String, KafkaPerson> streamTwo = streamsBuilder
                .stream(Topic.PERSON_TOPIC_TWO.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person));

        streamOne
                .merge(streamTwo)
                .to(Topic.PERSON_MERGE_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}