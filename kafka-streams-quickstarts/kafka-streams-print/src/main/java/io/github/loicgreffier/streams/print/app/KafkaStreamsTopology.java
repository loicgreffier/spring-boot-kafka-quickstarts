package io.github.loicgreffier.streams.print.app;

import static io.github.loicgreffier.streams.print.constant.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.print.constant.Topic.PERSON_TOPIC_TWO;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Printed;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the PERSON_TOPIC topic
     * and prints the result to the file specified by the {@code filePath} parameter.
     * The topology also reads from the PERSON_TOPIC_TWO topic and prints the result to the console.
     *
     * @param streamsBuilder the streams builder.
     * @param filePath       the file path.
     */
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

    /**
     * Formats the key and value into a string.
     *
     * @param key         the key.
     * @param kafkaPerson the value.
     * @return the formatted string.
     */
    private static String toOutput(String key, KafkaPerson kafkaPerson) {
        return String.format("Received key = %s, value = %s", key, kafkaPerson);
    }
}
