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

package io.github.loicgreffier.streams.leftjoin.stream.table.app;

import static io.github.loicgreffier.streams.leftjoin.stream.table.constant.Topic.COUNTRY_TOPIC;
import static io.github.loicgreffier.streams.leftjoin.stream.table.constant.Topic.USER_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC;
import static io.github.loicgreffier.streams.leftjoin.stream.table.constant.Topic.USER_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC;
import static io.github.loicgreffier.streams.leftjoin.stream.table.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaCountry;
import io.github.loicgreffier.avro.KafkaJoinUserCountry;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.leftjoin.stream.table.constant.StateStore;
import io.github.loicgreffier.streams.leftjoin.stream.table.serdes.SerdesUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KTable;
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
     * The topology reads from the USER_TOPIC topic and the COUNTRY_TOPIC topic as a table.
     * The stream is joined to the table by nationality with a left join.
     * The result is written to the USER_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC topic.
     *
     * <p>
     * A left join emits an output for each record in the primary stream. If there is no matching record in the
     * secondary stream, a null value is returned.
     * </p>
     *
     * @param streamsBuilder The streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        KTable<String, KafkaCountry> countryTable = streamsBuilder
            .table(COUNTRY_TOPIC, Materialized
                .<String, KafkaCountry, KeyValueStore<Bytes, byte[]>>as(StateStore.COUNTRY_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(SerdesUtils.getValueSerdes())
            );

        streamsBuilder
            .<String, KafkaUser>stream(USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
            .selectKey((key, user) -> user.getNationality().toString())
            .leftJoin(countryTable,
                (key, user, country) -> {
                    if (country != null) {
                        log.info("Joined {} {} {} to country {} by code {}", user.getId(),
                            user.getFirstName(), user.getLastName(),
                            country.getName(), key);
                    } else {
                        log.info("No matching country for {} {} {} with code {}", user.getId(),
                            user.getFirstName(), user.getLastName(), key);
                    }

                    return KafkaJoinUserCountry.newBuilder()
                        .setUser(user)
                        .setCountry(country)
                        .build();
                },
                Joined.with(
                    Serdes.String(),
                    SerdesUtils.getValueSerdes(),
                    SerdesUtils.getValueSerdes(),
                    USER_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC
                ))
            .to(USER_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC,
                Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
