package io.github.loicgreffier.streams.join.stream.stream.app;

import static io.github.loicgreffier.streams.join.stream.stream.constants.StateStore.PERSON_JOIN_STREAM_STREAM_STATE_STORE;
import static io.github.loicgreffier.streams.join.stream.stream.constants.Topic.PERSON_JOIN_STREAM_STREAM_REKEY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constants.Topic.PERSON_JOIN_STREAM_STREAM_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constants.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constants.Topic.PERSON_TOPIC_TWO;

import io.github.loicgreffier.avro.KafkaJoinPersons;
import io.github.loicgreffier.avro.KafkaPerson;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the
     * {@link io.github.loicgreffier.streams.join.stream.stream.constants.Topic#PERSON_TOPIC} topic
     * and the {@link
     * io.github.loicgreffier.streams.join.stream.stream.constants.Topic#PERSON_TOPIC_TWO}
     * topic. The stream is joined to the other stream by last name with an inner join.
     * The join window is 5 minutes and the grace period is 1 minute for late arriving events.
     * The result is written to the {@link
     * io.github.loicgreffier.streams.join.stream.stream.constants.Topic#PERSON_JOIN_STREAM_STREAM_TOPIC}
     * topic.
     *
     * @param streamsBuilder the streams builder.
     */
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
                    log.info("Joined {} and {} by last name {}", personLeft.getFirstName(),
                        personRight.getFirstName(), key);
                    return KafkaJoinPersons.newBuilder()
                        .setPersonOne(personLeft)
                        .setPersonTwo(personRight)
                        .build();
                },
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)),
                StreamJoined.<String, KafkaPerson, KafkaPerson>as(
                        PERSON_JOIN_STREAM_STREAM_STATE_STORE)
                    .withName(PERSON_JOIN_STREAM_STREAM_REKEY_TOPIC))
            .to(PERSON_JOIN_STREAM_STREAM_TOPIC);
    }
}
