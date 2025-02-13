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

package io.github.loicgreffier.streams.branch.app;

import static io.github.loicgreffier.streams.branch.constant.Topic.USER_BRANCH_A_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.USER_BRANCH_B_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.USER_BRANCH_DEFAULT_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
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
     * The topology reads from the USER_TOPIC topic.
     * Then, the stream is split into two branches:
     * <ul>
     *     <li>the first branch is filtered by the last name starting with "S".</li>
     *     <li>the second branch is filtered by the last name starting with "F".</li>
     *     <li>the default branch is used for all other last names.</li>
     * </ul>
     * The result is written to the USER_BRANCH_A_TOPIC topic, USER_BRANCH_B_TOPIC topic and
     * USER_BRANCH_DEFAULT_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        Map<String, KStream<String, KafkaUser>> branches = streamsBuilder
            .<String, KafkaUser>stream(USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
            .split(Named.as("BRANCH_"))
            .branch((key, value) -> value.getLastName().startsWith("S"),
                Branched.withFunction(KafkaStreamsTopology::toUppercase, "A"))
            .branch((key, value) -> value.getLastName().startsWith("F"), Branched.as("B"))
            .defaultBranch(Branched.withConsumer(stream -> stream
                .to(USER_BRANCH_DEFAULT_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()))));

        branches.get("BRANCH_A")
            .to(USER_BRANCH_A_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));

        branches.get("BRANCH_B")
            .to(USER_BRANCH_B_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /**
     * Converts the first and last name to uppercase.
     *
     * @param streamUser the stream of users.
     * @return the stream of users with uppercase first and last name.
     */
    private static KStream<String, KafkaUser> toUppercase(KStream<String, KafkaUser> streamUser) {
        return streamUser.mapValues(user -> {
            user.setFirstName(user.getFirstName().toUpperCase());
            user.setLastName(user.getLastName().toUpperCase());
            return user;
        });
    }
}
