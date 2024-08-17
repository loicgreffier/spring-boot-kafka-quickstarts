package io.github.loicgreffier.streams.production.exception.handler.app;

import static io.github.loicgreffier.streams.production.exception.handler.constant.Topic.PERSON_PRODUCTION_EXCEPTION_HANDLER_TOPIC;
import static io.github.loicgreffier.streams.production.exception.handler.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonWithEmail;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {
    private static final String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ";
    private static final int ONE_MEBIBYTE = 1048576;

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic and either:
     * <ul>
     *     <li>Populates the email field changing the record type from KafkaPerson to KafkaPersonWithEmail.
     *     As the email field is not nullable, it breaks the schema backward compatibility and triggers a serialization
     *     exception when registering the schema in the Schema Registry automatically.</li>
     *     <li>Populates the biography field with a large text that exceeds the maximum record size allowed by Kafka (1MiB).
     *     It triggers a production exception due to the record being too large.</li>
     *     </li>
     * </ul>
     * The population of the email field and the biography field is not triggered for all records to avoid generating
     * too many exceptions.
     * The result is written to the PERSON_PRODUCTION_EXCEPTION_HANDLER_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        StringBuilder stringBuilder = new StringBuilder();
        while (stringBuilder.toString().getBytes().length < ONE_MEBIBYTE) {
            stringBuilder.append(LOREM_IPSUM);
        }

        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .mapValues(person -> {
                if (person.getId() % 15 == 10) {
                    return KafkaPersonWithEmail.newBuilder()
                        .setId(person.getId())
                        .setFirstName(person.getFirstName())
                        .setLastName(person.getLastName())
                        .setEmail(person.getFirstName() + "." + person.getLastName() + "@mail.com")
                        .setNationality(person.getNationality())
                        .setBirthDate(person.getBirthDate())
                        .setBiography(person.getBiography())
                        .build();
                }

                if (person.getId() % 15 == 1) {
                    person.setBiography(stringBuilder.toString());
                }

                return person;
            })
            .to(PERSON_PRODUCTION_EXCEPTION_HANDLER_TOPIC);
    }
}
