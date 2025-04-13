/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.github.loicgreffier.streams.outerjoin.stream.stream.app;

import static io.github.loicgreffier.streams.outerjoin.stream.stream.constant.StateStore.USER_OUTER_JOIN_STREAM_STREAM_STORE;
import static io.github.loicgreffier.streams.outerjoin.stream.stream.constant.Topic.USER_OUTER_JOIN_STREAM_STREAM_REKEY_TOPIC;
import static io.github.loicgreffier.streams.outerjoin.stream.stream.constant.Topic.USER_OUTER_JOIN_STREAM_STREAM_TOPIC;
import static io.github.loicgreffier.streams.outerjoin.stream.stream.constant.Topic.USER_TOPIC;
import static io.github.loicgreffier.streams.outerjoin.stream.stream.constant.Topic.USER_TOPIC_TWO;

import io.github.loicgreffier.avro.KafkaJoinUsers;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.outerjoin.stream.stream.serdes.SerdesUtils;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the {@code USER_TOPIC} and {@code USER_TOPIC_TWO}
     * topics. The streams are joined by last name using an outer join with 5-minute symmetric join windows and a
     * 1-minute grace period. The result is written to the {@code USER_OUTER_JOIN_STREAM_STREAM_TOPIC} topic.
     *
     * <p>An outer join produces an output for each record in both streams. If no matching record is found in the other
     * stream, a null value is returned at the end of the join window plus the grace period. A new record is required to
     * advance the stream time and emit the null result.
     *
     * <p>{@link JoinWindows} are aligned to the record's timestamp. They are created each time a record is processed,
     * with bounds defined as [timestamp - before, timestamp + after]. An output is produced if a record in the
     * secondary stream has a timestamp within the window of a record in the primary stream, as shown below:
     *
     * <pre>
     * {@code stream1.ts - before <= stream2.ts AND stream2.ts <= stream1.ts + after}
     * </pre>
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaUser> streamTwo = streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC_TWO, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
                .selectKey((key, user) -> user.getLastName());

        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
                .selectKey((key, user) -> user.getLastName())
                .outerJoin(
                        streamTwo,
                        (key, userLeft, userRight) -> {
                            if (userLeft == null) {
                                log.info(
                                        "No matching user to the left for {} {} {}",
                                        userRight.getId(),
                                        userRight.getFirstName(),
                                        userRight.getLastName());
                            } else if (userRight == null) {
                                log.info(
                                        "No matching user to the right for {} {} {}",
                                        userLeft.getId(),
                                        userLeft.getFirstName(),
                                        userLeft.getLastName());
                            } else {
                                log.info(
                                        "Joined {} and {} by last name {}",
                                        userLeft.getFirstName(),
                                        userRight.getFirstName(),
                                        key);
                            }

                            return KafkaJoinUsers.newBuilder()
                                    .setUserOne(userLeft)
                                    .setUserTwo(userRight)
                                    .build();
                        },
                        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)),
                        StreamJoined.<String, KafkaUser, KafkaUser>with(
                                        Serdes.String(), SerdesUtils.getValueSerdes(), SerdesUtils.getValueSerdes())
                                .withName(USER_OUTER_JOIN_STREAM_STREAM_REKEY_TOPIC)
                                .withStoreName(USER_OUTER_JOIN_STREAM_STREAM_STORE))
                .to(USER_OUTER_JOIN_STREAM_STREAM_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
