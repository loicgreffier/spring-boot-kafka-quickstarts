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

package io.github.loicgreffier.streams.count.app;

import static io.github.loicgreffier.streams.count.constant.StateStore.USER_COUNT_STORE;
import static io.github.loicgreffier.streams.count.constant.Topic.GROUP_USER_BY_NATIONALITY_TOPIC;
import static io.github.loicgreffier.streams.count.constant.Topic.USER_COUNT_TOPIC;
import static io.github.loicgreffier.streams.count.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.count.serdes.SerdesUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the USER_TOPIC topic, groups by nationality and counts the number of users.
     * The result is written to the USER_COUNT_TOPIC topic.
     *
     * @param streamsBuilder The streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaUser>stream(USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
            .groupBy((key, user) -> user.getNationality().toString(),
                Grouped.with(GROUP_USER_BY_NATIONALITY_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
            .count(Materialized
                .<String, Long, KeyValueStore<Bytes, byte[]>>as(USER_COUNT_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()))
            .toStream()
            .to(USER_COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }
}
