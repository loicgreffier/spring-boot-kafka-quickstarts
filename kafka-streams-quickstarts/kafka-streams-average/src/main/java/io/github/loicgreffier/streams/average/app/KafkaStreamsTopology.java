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

package io.github.loicgreffier.streams.average.app;

import static io.github.loicgreffier.streams.average.constant.StateStore.USER_AVERAGE_STORE;
import static io.github.loicgreffier.streams.average.constant.Topic.GROUP_USER_BY_NATIONALITY_TOPIC;
import static io.github.loicgreffier.streams.average.constant.Topic.USER_AVERAGE_TOPIC;
import static io.github.loicgreffier.streams.average.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaAverageAge;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.average.app.aggregator.AgeAggregator;
import io.github.loicgreffier.streams.average.serdes.SerdesUtils;
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
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the USER_TOPIC topic, groups by nationality and aggregates the age sum and count.
     * Then, the average age is computed.
     * The result is written to the USER_AVERAGE_TOPIC topic.
     *
     * @param streamsBuilder The streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaUser>stream(USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
            .groupBy((key, user) -> user.getNationality().toString(),
                Grouped.with(GROUP_USER_BY_NATIONALITY_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
            .aggregate(
                () -> new KafkaAverageAge(0L, 0L),
                new AgeAggregator(),
                Materialized
                    .<String, KafkaAverageAge, KeyValueStore<Bytes, byte[]>>as(USER_AVERAGE_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SerdesUtils.getValueSerdes())
            )
            .mapValues(value -> value.getAgeSum() / value.getCount())
            .toStream()
            .to(USER_AVERAGE_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    /**
     * Private constructor.
     */
    private KafkaStreamsTopology() {}
}
