package io.github.loicgreffier.streams.branch.app;

import static io.github.loicgreffier.streams.branch.constant.Topic.PERSON_BRANCH_A_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.PERSON_BRANCH_B_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.PERSON_BRANCH_DEFAULT_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.branch.serdes.SerdesUtils;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic.
     * Then, the stream is split into two branches:
     * <ul>
     *     <li>the first branch is filtered by the last name starting with "S".</li>
     *     <li>the second branch is filtered by the last name starting with "F".</li>
     *     <li>the default branch is used for all other last names.</li>
     * </ul>
     * The result is written to the PERSON_BRANCH_A_TOPIC topic, PERSON_BRANCH_B_TOPIC topic and
     * PERSON_BRANCH_DEFAULT_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        Map<String, KStream<String, KafkaPerson>> branches = streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .split(Named.as("BRANCH_"))
            .branch((key, value) -> value.getLastName().startsWith("S"),
                Branched.withFunction(KafkaStreamsTopology::toUppercase, "A"))
            .branch((key, value) -> value.getLastName().startsWith("F"), Branched.as("B"))
            .defaultBranch(Branched.withConsumer(stream -> stream
                .to(PERSON_BRANCH_DEFAULT_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()))));

        branches.get("BRANCH_A")
            .to(PERSON_BRANCH_A_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));

        branches.get("BRANCH_B")
            .to(PERSON_BRANCH_B_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /**
     * Converts the first and last name to uppercase.
     *
     * @param streamPerson the stream of persons.
     * @return the stream of persons with uppercase first and last name.
     */
    private static KStream<String, KafkaPerson> toUppercase(KStream<String, KafkaPerson> streamPerson) {
        return streamPerson.mapValues(person -> {
            person.setFirstName(person.getFirstName().toUpperCase());
            person.setLastName(person.getLastName().toUpperCase());
            return person;
        });
    }
}
