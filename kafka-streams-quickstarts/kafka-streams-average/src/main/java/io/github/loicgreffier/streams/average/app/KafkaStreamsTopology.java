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

import static io.github.loicgreffier.streams.average.constant.StateStore.ORDER_AVERAGE_STORE;
import static io.github.loicgreffier.streams.average.constant.Topic.GROUP_ORDER_BY_CUSTOMER_TOPIC;
import static io.github.loicgreffier.streams.average.constant.Topic.ORDER_AVERAGE_TOPIC;
import static io.github.loicgreffier.streams.average.constant.Topic.ORDER_TOPIC;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.OrderAverageAmount;
import io.github.loicgreffier.streams.average.app.aggregator.AmountAggregator;
import io.github.loicgreffier.streams.average.serdes.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
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
     * <p>This topology reads records from the {@code ORDER_TOPIC} topic, groups the records by customer id, and
     * performs an aggregation of the total amount sum and count for each customer. Afterward, the average order amount
     * for each group is computed by dividing the total amount sum by the count. The result, which includes the average
     * order amount by customer, is written to the {@code ORDER_AVERAGE_TOPIC} topic.
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder.<String, Order>stream(ORDER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, order) -> log.info("Processing key = {}, value = {}", key, order))
                .groupBy(
                        (_, order) -> String.valueOf(order.getCustomerId()),
                        Grouped.with(GROUP_ORDER_BY_CUSTOMER_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()))
                .aggregate(
                        () -> new OrderAverageAmount(0L, 0.0),
                        new AmountAggregator(),
                        Materialized.<String, OrderAverageAmount, KeyValueStore<Bytes, byte[]>>as(ORDER_AVERAGE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesUtils.getValueSerdes()))
                .mapValues(value -> Math.round(value.getAmountSum() / value.getCount() * 100) / 100.0)
                .toStream()
                .to(ORDER_AVERAGE_TOPIC, Produced.with(Serdes.String(), Serdes.Double()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
