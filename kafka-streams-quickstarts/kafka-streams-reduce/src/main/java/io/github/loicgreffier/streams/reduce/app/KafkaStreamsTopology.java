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
package io.github.loicgreffier.streams.reduce.app;

import static io.github.loicgreffier.streams.reduce.constant.StateStore.USER_REDUCE_STORE;
import static io.github.loicgreffier.streams.reduce.constant.Topic.GROUP_USER_BY_NATIONALITY_TOPIC;
import static io.github.loicgreffier.streams.reduce.constant.Topic.USER_REDUCE_TOPIC;
import static io.github.loicgreffier.streams.reduce.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.reduce.app.reducer.MaxAgeReducer;
import io.github.loicgreffier.streams.reduce.serdes.SerdesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the {@code USER_TOPIC} topic, groups by nationality
     * and reduces the stream to the user with the max age. The result is written to the {@code USER_REDUCE_TOPIC}
     * topic.
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
                .groupBy(
                        (key, user) -> user.getNationality().toString(),
                        Grouped.with(GROUP_USER_BY_NATIONALITY_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
                .reduce(
                        new MaxAgeReducer(),
                        Materialized.<String, KafkaUser, KeyValueStore<Bytes, byte[]>>as(USER_REDUCE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesUtils.getValueSerdes()))
                .toStream()
                .to(USER_REDUCE_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
