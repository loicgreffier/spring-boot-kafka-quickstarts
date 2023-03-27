package io.github.loicgreffier.streams.branch.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.branch.constants.Topic;
import io.github.loicgreffier.streams.branch.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.util.Map;

@Slf4j
public class KafkaStreamsBranchTopology {
    private KafkaStreamsBranchTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        Map<String, KStream<String, KafkaPerson>> branches = streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .split(Named.as("BRANCH_"))
                .branch((key, value) -> value.getLastName().startsWith("A"),
                        Branched.withFunction(KafkaStreamsBranchTopology::toUppercase, "A"))
                .branch((key, value) -> value.getLastName().startsWith("B"),
                        Branched.as("B"))
                .defaultBranch(Branched.withConsumer(stream -> stream
                        .to(Topic.PERSON_BRANCH_DEFAULT_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()))));

        branches.get("BRANCH_A")
                .to(Topic.PERSON_BRANCH_A_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));

        branches.get("BRANCH_B")
                .to(Topic.PERSON_BRANCH_B_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }

    private static KStream<String, KafkaPerson> toUppercase(KStream<String, KafkaPerson> streamPerson) {
        return streamPerson.mapValues(person -> {
            person.setFirstName(person.getFirstName().toUpperCase());
            person.setLastName(person.getLastName().toUpperCase());
            return person;
        });
    }
}
