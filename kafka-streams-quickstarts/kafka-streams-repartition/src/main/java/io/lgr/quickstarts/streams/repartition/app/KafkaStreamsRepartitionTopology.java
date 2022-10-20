package io.lgr.quickstarts.streams.repartition.app;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.repartition.constants.Topic;
import io.lgr.quickstarts.streams.repartition.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Repartitioned;

@Slf4j
public class KafkaStreamsRepartitionTopology {
    private KafkaStreamsRepartitionTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .repartition(Repartitioned
                        .<String, KafkaPerson>as(Topic.PERSON_TOPIC.toString())
                        .withNumberOfPartitions(3)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(CustomSerdes.getValueSerdes()));
    }
}
