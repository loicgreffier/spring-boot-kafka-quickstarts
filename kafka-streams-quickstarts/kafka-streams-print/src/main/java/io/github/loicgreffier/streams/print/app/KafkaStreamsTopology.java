package io.github.loicgreffier.streams.print.app;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Printed;

import static io.github.loicgreffier.streams.print.constants.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.print.constants.Topic.PERSON_TOPIC_TWO;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {
    public static void topology(StreamsBuilder streamsBuilder, String filePath) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .print(Printed.<String, KafkaPerson>toFile(filePath)
                        .withKeyValueMapper(KafkaStreamsTopology::toOutput)
                        .withLabel(PERSON_TOPIC));

        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC_TWO)
                .print(Printed.<String, KafkaPerson>toSysOut()
                        .withKeyValueMapper(KafkaStreamsTopology::toOutput)
                        .withLabel(PERSON_TOPIC_TWO));
    }

    private static String toOutput(String key, KafkaPerson kafkaPerson) {
        return String.format("Received key = %s, value = %s", key, kafkaPerson);
    }
}
