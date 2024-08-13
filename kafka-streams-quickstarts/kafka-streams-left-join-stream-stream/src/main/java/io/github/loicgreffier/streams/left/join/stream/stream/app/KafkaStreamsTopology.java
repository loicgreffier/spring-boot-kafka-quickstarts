package io.github.loicgreffier.streams.left.join.stream.stream.app;

import static io.github.loicgreffier.streams.left.join.stream.stream.constant.StateStore.PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE;
import static io.github.loicgreffier.streams.left.join.stream.stream.constant.Topic.PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.stream.constant.Topic.PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.stream.constant.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.left.join.stream.stream.constant.Topic.PERSON_TOPIC_TWO;

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
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic and the PERSON_TOPIC_TWO topic.
     * The stream is joined to the other stream by last name with a left join and 5-minute symmetric join windows
     * with 1-minute grace period.
     * The result is written to the PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC topic.
     * <p>
     * A left join emits an output for each record in the primary stream. If there is no matching record in the
     * secondary stream, a null value is returned at the end of the join window + grace period.
     * A new record to process is required to make the stream time advance and emit the null result.
     * </p>
     * <p>
     * {@link JoinWindows} are aligned to the records timestamp.
     * They are created each time a record is processed and are bounded such as [timestamp - before, timestamp + after].
     * An output is produced if a record is found in the secondary stream that has a timestamp within the window of a
     * record in the primary stream such as:
     * <pre>
     * {@code stream1.ts - before <= stream2.ts AND stream2.ts <= stream1.ts + after}
     * </pre>
     * </p>
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
            .leftJoin(streamTwo,
                (key, personLeft, personRight) -> {
                    if (personRight != null) {
                        log.info("Joined {} and {} by last name {}", personLeft.getFirstName(),
                            personRight.getFirstName(), key);
                    } else {
                        log.info("No matching person for {} {} {}", personLeft.getId(),
                            personLeft.getFirstName(), personLeft.getLastName());
                    }

                    return KafkaJoinPersons.newBuilder()
                        .setPersonOne(personLeft)
                        .setPersonTwo(personRight)
                        .build();
                },
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)),
                StreamJoined.<String, KafkaPerson, KafkaPerson>as(PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE)
                    .withName(PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC)
            )
            .to(PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC);
    }
}
