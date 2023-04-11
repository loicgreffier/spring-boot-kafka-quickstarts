package io.github.loicgreffier.streams.process.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.process.app.processor.KeyValueProcessor;
import io.github.loicgreffier.streams.process.constants.Topic;
import io.github.loicgreffier.streams.process.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class KafkaStreamsProcessTopology {
    private KafkaStreamsProcessTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .process(new KeyValueProcessor())
                .to(Topic.PERSON_PROCESS_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
