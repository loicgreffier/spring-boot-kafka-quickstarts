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

import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.StateStore.ORDER_AGGREGATE_SLIDING_WINDOW_STORE;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.GROUP_ORDER_BY_CUSTOMER_TOPIC;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.ORDER_AGGREGATE_SLIDING_WINDOW_TOPIC;
import static io.github.loicgreffier.streams.aggregate.sliding.window.constant.Topic.ORDER_TOPIC;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.OrderAggregate;
import io.github.loicgreffier.streams.aggregate.sliding.window.app.aggregator.OrderAggregator;
import io.github.loicgreffier.streams.aggregate.sliding.window.serdes.SerdesUtils;
import java.time.Duration;
import java.util.ArrayList;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Kafka Streams topology. */
public class KafkaStreamsTopology {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsTopology.class);

    /**
     * Builds the Kafka Streams topology.
     *
     * <p>This topology reads records from the {@code ORDER_TOPIC} topic, selects the customer id of the order as the
     * key, groups the records by key, and aggregates orders by customer id using 5-minute sliding windows with a
     * 1-minute grace period. A new key is generated with the window's start and end times. The aggregated results are
     * written to the {@code ORDER_AGGREGATE_SLIDING_WINDOW_TOPIC} topic.
     *
     * <p>{@link org.apache.kafka.streams.kstream.SlidingWindows} are aligned to the record's timestamp. Each time a
     * record is processed, a new window is created. The window is bounded as follows:
     *
     * <ul>
     *   <li>[timestamp - window size, timestamp]
     *   <li>[timestamp + 1ms, timestamp + window size + 1ms]
     * </ul>
     *
     * <p>The sliding window looks backward in time. Two records are considered to be in the same window if the
     * difference between their timestamps is less than the window size.
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     * @see org.apache.kafka.streams.kstream.internals.KStreamSlidingWindowAggregate
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder.<String, Order>stream(ORDER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, order) -> log.info("Processing key = {}, value = {}", key, order))
                .selectKey((_, order) -> String.valueOf(order.getCustomerId()))
                .groupByKey(Grouped.with(GROUP_ORDER_BY_CUSTOMER_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)))
                .aggregate(
                        () -> new OrderAggregate(new ArrayList<>()),
                        new OrderAggregator(),
                        Materialized.<String, OrderAggregate, WindowStore<Bytes, byte[]>>as(
                                        ORDER_AGGREGATE_SLIDING_WINDOW_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesUtils.getValueSerdes()))
                .toStream()
                .selectKey((key, _) -> key.key() + "@" + key.window().startTime() + "->"
                        + key.window().endTime())
                .to(ORDER_AGGREGATE_SLIDING_WINDOW_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
