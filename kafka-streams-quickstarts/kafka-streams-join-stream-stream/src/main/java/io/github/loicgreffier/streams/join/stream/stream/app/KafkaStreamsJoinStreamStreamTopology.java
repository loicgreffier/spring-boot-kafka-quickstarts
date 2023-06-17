package io.github.loicgreffier.streams.join.stream.stream.app;

import io.github.loicgreffier.avro.KafkaJoinPersons;
import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

import java.time.Duration;

import static io.github.loicgreffier.streams.join.stream.stream.constants.StateStore.PERSON_JOIN_STREAM_STREAM_STATE_STORE;
import static io.github.loicgreffier.streams.join.stream.stream.constants.Topic.*;

@Slf4j
public class KafkaStreamsJoinStreamStreamTopology {
    private KafkaStreamsJoinStreamStreamTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> streamTwo = streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC_TWO)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName());

        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
                .selectKey((key, person) -> person.getLastName())
                .join(streamTwo,
                        (key, personLeft, personRight) -> {
                            log.info("Joined {} and {} by last name {}", personLeft.getFirstName(), personRight.getFirstName(), key);
                            return KafkaJoinPersons.newBuilder()
                                    .setPersonOne(personLeft)
                                    .setPersonTwo(personRight)
                                    .build();
                        },
                        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(2), Duration.ofSeconds(30)),
                        StreamJoined.<String, KafkaPerson, KafkaPerson>as(PERSON_JOIN_STREAM_STREAM_STATE_STORE)
                                .withName(PERSON_JOIN_STREAM_STREAM_REKEY_TOPIC))
                .to(PERSON_JOIN_STREAM_STREAM_TOPIC);
    }
}
