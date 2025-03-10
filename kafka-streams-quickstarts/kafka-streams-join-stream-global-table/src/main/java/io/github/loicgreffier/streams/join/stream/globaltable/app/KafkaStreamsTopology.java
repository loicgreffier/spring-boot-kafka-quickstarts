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
package io.github.loicgreffier.streams.join.stream.globaltable.app;

import static io.github.loicgreffier.streams.join.stream.globaltable.constant.StateStore.COUNTRY_STORE;
import static io.github.loicgreffier.streams.join.stream.globaltable.constant.Topic.COUNTRY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.globaltable.constant.Topic.USER_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC;
import static io.github.loicgreffier.streams.join.stream.globaltable.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinUserCountry;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.join.stream.globaltable.serdes.SerdesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the USER_TOPIC topic and the COUNTRY_TOPIC topic as a
     * global table. The stream is joined to the global table by nationality with an inner join. The result is written
     * to the USER_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC topic.
     *
     * <p>An inner join emits an output when both streams have records with the same key.
     *
     * @param streamsBuilder The streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        GlobalKTable<String, KafkaCountry> countryGlobalTable = streamsBuilder.globalTable(
                COUNTRY_TOPIC,
                Materialized.<String, KafkaCountry, KeyValueStore<Bytes, byte[]>>as(COUNTRY_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SerdesUtils.getValueSerdes()));

        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
                .join(countryGlobalTable, (key, user) -> user.getNationality().toString(), (user, country) -> {
                    log.info(
                            "Joined {} {} {} to country {} by code {}",
                            user.getId(),
                            user.getFirstName(),
                            user.getLastName(),
                            country.getName(),
                            country.getCode());
                    return KafkaJoinUserCountry.newBuilder()
                            .setUser(user)
                            .setCountry(country)
                            .build();
                })
                .to(
                        USER_COUNTRY_JOIN_STREAM_GLOBAL_TABLE_TOPIC,
                        Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
