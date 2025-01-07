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

package io.github.loicgreffier.streams.join.stream.stream.app;

import static io.github.loicgreffier.streams.join.stream.stream.constant.StateStore.PERSON_JOIN_STREAM_STREAM_STORE;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.PERSON_JOIN_STREAM_STREAM_REKEY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.PERSON_JOIN_STREAM_STREAM_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.PERSON_TOPIC_TWO;

import io.github.loicgreffier.avro.KafkaJoinPersons;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.join.stream.stream.serdes.SerdesUtils;
import java.time.Duration;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic and the PERSON_TOPIC_TWO topic.
     * The stream is joined to the other stream by last name with an inner join and 5-minute symmetric join windows
     * with 1-minute grace period.
     * The result is written to the PERSON_JOIN_STREAM_STREAM_TOPIC topic.
     *
     * <p>
     * An inner join emits an output when both streams have records with the same key.
     * </p>
     *
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
            .<String, KafkaPerson>stream(PERSON_TOPIC_TWO, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .selectKey((key, person) -> person.getLastName());

        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
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
                StreamJoined
                    .<String, KafkaPerson, KafkaPerson>with(
                        Serdes.String(),
                        SerdesUtils.getValueSerdes(),
                        SerdesUtils.getValueSerdes()
                    )
                    .withName(PERSON_JOIN_STREAM_STREAM_REKEY_TOPIC)
                    .withStoreName(PERSON_JOIN_STREAM_STREAM_STORE))
            .to(PERSON_JOIN_STREAM_STREAM_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
