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
package io.github.loicgreffier.streams.cogroup.app;

import static io.github.loicgreffier.streams.cogroup.constant.StateStore.ORDER_COGROUP_AGGREGATE_STORE;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.GROUP_ORDER_BY_CUSTOMER_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.GROUP_ORDER_BY_CUSTOMER_TOPIC_TWO;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.ORDER_COGROUP_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.ORDER_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.ORDER_TOPIC_TWO;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.OrderAggregate;
import io.github.loicgreffier.streams.cogroup.app.aggregator.OrderAggregator;
import io.github.loicgreffier.streams.cogroup.serdes.SerdesUtils;
import java.util.ArrayList;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Kafka Streams topology. */
public class KafkaStreamsTopology {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsTopology.class);

    /**
     * Builds the Kafka Streams topology.
     *
     * <p>This topology reads from the {@code ORDER_TOPIC} and {@code ORDER_TOPIC_TWO} topics, groups both streams by
     * last id and cogroups them so a single aggregate per customer is built out of both sources. The result is written
     * to the {@code ORDER_COGROUP_TOPIC} topic.
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        final OrderAggregator aggregator = new OrderAggregator();

        final KGroupedStream<String, Order> groupedStreamOne = streamsBuilder.<String, Order>stream(
                        ORDER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, order) -> log.info("Processing key = {}, value = {}", key, order))
                .groupBy(
                        (_, order) -> String.valueOf(order.getCustomerId()),
                        Grouped.with(GROUP_ORDER_BY_CUSTOMER_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()));

        final KGroupedStream<String, Order> groupedStreamTwo = streamsBuilder.<String, Order>stream(
                        ORDER_TOPIC_TWO, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, order) -> log.info("Processing key = {}, value = {}", key, order))
                .groupBy(
                        (_, order) -> String.valueOf(order.getCustomerId()),
                        Grouped.with(GROUP_ORDER_BY_CUSTOMER_TOPIC_TWO, Serdes.String(), SerdesUtils.getValueSerdes()));

        groupedStreamOne
                .cogroup(aggregator)
                .cogroup(groupedStreamTwo, aggregator)
                .aggregate(
                        () -> new OrderAggregate(new ArrayList<>()),
                        Materialized.<String, OrderAggregate, KeyValueStore<Bytes, byte[]>>as(
                                        ORDER_COGROUP_AGGREGATE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesUtils.getValueSerdes()))
                .toStream()
                .to(ORDER_COGROUP_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
