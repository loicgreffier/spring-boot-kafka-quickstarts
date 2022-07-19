package io.lgr.quickstarts.streams.selectkey.app;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.selectkey.constants.Topic;
import io.lgr.quickstarts.streams.selectkey.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class KafkaStreamsSelectKeyTopology {
    private KafkaStreamsSelectKeyTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName())
                .to(Topic.PERSON_SELECT_KEY_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getSerdes()));
    }
}
