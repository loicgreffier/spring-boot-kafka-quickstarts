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
package io.github.loicgreffier.streams.aggregate.sliding.window.app;

import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.StateStore.USER_AGGREGATE_SLIDING_WINDOW_STORE;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.GROUP_USER_BY_LAST_NAME_TOPIC;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.USER_AGGREGATE_SLIDING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.avro.KafkaUserAggregate;
import io.github.loicgreffier.streams.aggregate.sliding.window.app.aggregator.KafkaUserAggregator;
import io.github.loicgreffier.streams.aggregate.sliding.window.serdes.SerdesUtils;
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
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.state.WindowStore;

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the USER_TOPIC topic, selects the key as the last name
     * of the user, groups by key and aggregates users by last name in 5-minute tumbling windows with 1-minute grace
     * period. A new key is generated with the window start and end time. The result is written to the
     * USER_AGGREGATE_SLIDING_WINDOW_TOPIC topic.
     *
     * <p>{@link org.apache.kafka.streams.kstream.SlidingWindows} are aligned to the records timestamp. They are created
     * each time a record is processed and when a record becomes older than the window size. They are bounded such as:
     *
     * <ul>
     *   <li>[timestamp - window size, timestamp]
     *   <li>[timestamp + 1ms, timestamp + window size + 1ms]
     * </ul>
     *
     * They are looking backward in time. Two records are in the same window if the difference between their timestamps
     * is less than the window size.
     *
     * @param streamsBuilder The streams builder.
     * @see org.apache.kafka.streams.kstream.internals.KStreamSlidingWindowAggregate
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
                .selectKey((key, user) -> user.getLastName())
                .groupByKey(Grouped.with(GROUP_USER_BY_LAST_NAME_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)))
                .aggregate(
                        () -> new KafkaUserAggregate(new ArrayList<>()),
                        new KafkaUserAggregator(),
                        Materialized.<String, KafkaUserAggregate, WindowStore<Bytes, byte[]>>as(
                                        USER_AGGREGATE_SLIDING_WINDOW_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesUtils.getValueSerdes()))
                .toStream()
                .selectKey((key, groupedUsers) -> key.key() + "@" + key.window().startTime() + "->"
                        + key.window().endTime())
                .to(USER_AGGREGATE_SLIDING_WINDOW_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
