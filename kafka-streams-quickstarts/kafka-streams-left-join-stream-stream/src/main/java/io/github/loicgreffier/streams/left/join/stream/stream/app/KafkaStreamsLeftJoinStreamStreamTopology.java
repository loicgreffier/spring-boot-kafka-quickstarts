package io.github.loicgreffier.streams.left.join.stream.stream.app;

import io.github.loicgreffier.streams.left.join.stream.stream.constants.StateStore;
import io.github.loicgreffier.streams.left.join.stream.stream.constants.Topic;
import io.github.loicgreffier.streams.left.join.stream.stream.serdes.CustomSerdes;
import io.github.loicgreffier.avro.KafkaJoinPersons;
import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

@Slf4j
public class KafkaStreamsLeftJoinStreamStreamTopology {
    private KafkaStreamsLeftJoinStreamStreamTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> streamTwo = streamsBuilder
                .stream(Topic.PERSON_TOPIC_TWO.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName());

        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName())
                .leftJoin(streamTwo,
                        (key, personLeft, personRight) -> {
                            if (personRight != null) {
                                log.info("Joined {} and {} by last name {}", personLeft.getFirstName(), personRight.getFirstName(), key);
                            } else {
                                log.info("No matching person for {} {} {}", personLeft.getId(), personLeft.getFirstName(), personLeft.getLastName());
                            }

                            return KafkaJoinPersons.newBuilder()
                                    .setPersonOne(personLeft)
                                    .setPersonTwo(personRight)
                                    .build();
                        },
                        JoinWindows
                                .ofTimeDifferenceAndGrace(Duration.ofMinutes(2), Duration.ofSeconds(30)),
                        StreamJoined
                                .<String, KafkaPerson, KafkaPerson>with(Serdes.String(), CustomSerdes.getValueSerdes(), CustomSerdes.getValueSerdes())
                                .withName(Topic.PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC.toString())
                                .withStoreName(StateStore.PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE.toString())
                )
                .to(Topic.PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
