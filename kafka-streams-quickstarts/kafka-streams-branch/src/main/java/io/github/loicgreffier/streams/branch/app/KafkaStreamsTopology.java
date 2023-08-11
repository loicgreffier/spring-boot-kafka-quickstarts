package io.github.loicgreffier.streams.branch.app;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.Map;

import static io.github.loicgreffier.streams.branch.constants.Topic.*;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {
    public static void topology(StreamsBuilder streamsBuilder) {
        Map<String, KStream<String, KafkaPerson>> branches = streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .split(Named.as("BRANCH_"))
                .branch((key, value) -> value.getLastName().startsWith("A"),
                        Branched.withFunction(KafkaStreamsTopology::toUppercase, "A"))
                .branch((key, value) ->
                        value.getLastName().startsWith("B"), Branched.as("B"))
                .defaultBranch(Branched.withConsumer(stream ->
                        stream.to(PERSON_BRANCH_DEFAULT_TOPIC)));

        branches.get("BRANCH_A")
                .to(PERSON_BRANCH_A_TOPIC);

        branches.get("BRANCH_B")
                .to(PERSON_BRANCH_B_TOPIC);
    }

    private static KStream<String, KafkaPerson> toUppercase(KStream<String, KafkaPerson> streamPerson) {
        return streamPerson.mapValues(person -> {
            person.setFirstName(person.getFirstName().toUpperCase());
            person.setLastName(person.getLastName().toUpperCase());
            return person;
        });
    }
}
