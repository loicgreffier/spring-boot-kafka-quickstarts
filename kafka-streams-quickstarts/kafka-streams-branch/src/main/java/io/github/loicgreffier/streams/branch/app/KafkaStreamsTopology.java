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
package io.github.loicgreffier.streams.branch.app;

import static io.github.loicgreffier.streams.branch.constant.Topic.ORDER_BRANCH_A_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.ORDER_BRANCH_B_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.ORDER_BRANCH_DEFAULT_TOPIC;
import static io.github.loicgreffier.streams.branch.constant.Topic.ORDER_TOPIC;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.streams.branch.serdes.SerdesUtils;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Kafka Streams topology. */
public class KafkaStreamsTopology {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsTopology.class);

    /**
     * Builds the Kafka Streams topology.
     *
     * <p>This topology reads records from the {@code ORDER_TOPIC} topic, then splits the stream into three branches:
     *
     * <ul>
     *   <li>The first branch filters orders with an amount greater than or equal to 1000 and applies a 10% discount.
     *   <li>The second branch filters orders with an amount greater than or equal to 100.
     *   <li>The default branch is used for all other orders.
     * </ul>
     *
     * <p>The filtered records are written to the following topics:
     *
     * <ul>
     *   <li>{@code ORDER_BRANCH_A_TOPIC} for orders with an amount greater than or equal to 1000.
     *   <li>{@code ORDER_BRANCH_B_TOPIC} for orders with an amount greater than or equal to 100.
     *   <li>{@code ORDER_BRANCH_DEFAULT_TOPIC} for all other orders.
     * </ul>
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        Map<String, KStream<String, Order>> branches = streamsBuilder.<String, Order>stream(
                        ORDER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, order) -> log.info("Processing key = {}, value = {}", key, order))
                .split(Named.as("BRANCH_"))
                .branch(
                        (_, order) -> order.getAmount() >= 1000,
                        Branched.withFunction(KafkaStreamsTopology::applyDiscount, "A"))
                .branch((_, order) -> order.getAmount() >= 100, Branched.as("B"))
                .defaultBranch(Branched.withConsumer(stream -> stream.to(
                        ORDER_BRANCH_DEFAULT_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()))));

        branches.get("BRANCH_A").to(ORDER_BRANCH_A_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));

        branches.get("BRANCH_B").to(ORDER_BRANCH_B_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /**
     * Applies a 10% discount to the order amount.
     *
     * @param streamOrder The stream of orders.
     * @return The stream of orders with the discounted amount.
     */
    private static KStream<String, Order> applyDiscount(KStream<String, Order> streamOrder) {
        return streamOrder.mapValues(order -> {
            order.setAmount(Math.round(order.getAmount() * 0.9 * 100) / 100.0);
            return order;
        });
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
