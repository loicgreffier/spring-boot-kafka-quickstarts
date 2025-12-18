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
package io.github.loicgreffier.streams.aggregate.hopping.window.app;

import static io.github.loicgreffier.streams.aggregate.hopping.window.constant.StateStore.USER_AGGREGATE_HOPPING_WINDOW_STORE;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constant.Topic.GROUP_USER_BY_LAST_NAME_TOPIC;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constant.Topic.USER_AGGREGATE_HOPPING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.hopping.window.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.avro.KafkaUserAggregate;
import io.github.loicgreffier.streams.aggregate.hopping.window.app.aggregator.UserAggregator;
import io.github.loicgreffier.streams.aggregate.hopping.window.serdes.SerdesUtils;
import java.time.Duration;
import java.util.ArrayList;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     *
     * <p>This topology reads records from the {@code USER_TOPIC} topic, selects the last name of the user as the key,
     * groups the records by key, and aggregates users by last name using hopping windows. The hopping windows are 5
     * minutes in length, with a 1-minute grace period and a 2-minute advance period. A new key is generated with the
     * window's start and end times. The aggregation results are written to the
     * {@code USER_AGGREGATE_HOPPING_WINDOW_TOPIC} topic.
     *
     * <p>Hopping windows are aligned to the epoch (1970-01-01T00:00:00Z). A new window of 5 minutes is created every 2
     * minutes, as long as the stream time advances. A record belongs to a hopping window if its timestamp is within the
     * range [windowStart, windowEnd).
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
                .selectKey((_, user) -> user.getLastName())
                .groupByKey(Grouped.with(GROUP_USER_BY_LAST_NAME_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1))
                        .advanceBy(Duration.ofMinutes(2)))
                .aggregate(
                        () -> new KafkaUserAggregate(new ArrayList<>()),
                        new UserAggregator(),
                        Materialized.<String, KafkaUserAggregate, WindowStore<Bytes, byte[]>>as(
                                        USER_AGGREGATE_HOPPING_WINDOW_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesUtils.getValueSerdes()))
                .toStream()
                .selectKey((key, _) -> key.key() + "@" + key.window().startTime() + "->"
                        + key.window().endTime())
                .to(USER_AGGREGATE_HOPPING_WINDOW_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
