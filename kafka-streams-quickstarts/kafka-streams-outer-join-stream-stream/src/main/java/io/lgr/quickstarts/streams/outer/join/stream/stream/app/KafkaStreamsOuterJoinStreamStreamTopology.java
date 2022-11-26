package io.lgr.quickstarts.streams.outer.join.stream.stream.app;

import io.lgr.quickstarts.avro.KafkaJoinPersons;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.outer.join.stream.stream.constants.StateStore;
import io.lgr.quickstarts.streams.outer.join.stream.stream.constants.Topic;
import io.lgr.quickstarts.streams.outer.join.stream.stream.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

@Slf4j
public class KafkaStreamsOuterJoinStreamStreamTopology {
    private KafkaStreamsOuterJoinStreamStreamTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> streamTwo = streamsBuilder
                .stream(Topic.PERSON_TOPIC_TWO.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName());

        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName())
                .outerJoin(streamTwo,
                        (key, personLeft, personRight) -> {
                            if (personLeft == null) {
                                log.info("No matching person to the left for {} {} {}", personRight.getId(), personRight.getFirstName(), personRight.getLastName());
                            } else if (personRight == null) {
                                log.info("No matching person to the right for {} {} {}", personLeft.getId(), personLeft.getFirstName(), personLeft.getLastName());
                            } else {
                                log.info("Joined {} and {} by last name {}", personLeft.getFirstName(), personRight.getFirstName(), key);
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
                                .withName(Topic.PERSON_OUTER_JOIN_STREAM_STREAM_REKEY_TOPIC.toString())
                                .withStoreName(StateStore.PERSON_OUTER_JOIN_STREAM_STREAM_STATE_STORE.toString())
                )
                .to(Topic.PERSON_OUTER_JOIN_STREAM_STREAM_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
