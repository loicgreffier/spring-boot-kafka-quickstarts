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
package io.github.loicgreffier.streams.join.stream.stream.app;

import static io.github.loicgreffier.streams.join.stream.stream.constant.StateStore.ORDER_DELIVERY_JOIN_STREAM_STREAM_STORE;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.DELIVERY_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.ORDER_DELIVERY_JOIN_STREAM_STREAM_TOPIC;
import static io.github.loicgreffier.streams.join.stream.stream.constant.Topic.ORDER_TOPIC;

import io.github.loicgreffier.avro.Delivery;
import io.github.loicgreffier.avro.JoinOrderDelivery;
import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.streams.join.stream.stream.serdes.SerdesUtils;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Kafka Streams topology. */
public class KafkaStreamsTopology {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsTopology.class);

    /**
     * Builds the Kafka Streams topology.
     *
     * <p>This topology reads from the {@code ORDER_TOPIC} topic and the {@code DELIVERY_TOPIC} topic. Both streams are
     * keyed by order id, so they are co-partitioned and can be joined without being re-keyed. The orders are joined to
     * the deliveries using an inner join, with a 5-minute symmetric join window and a 1-minute grace period. The result
     * is written to the {@code ORDER_DELIVERY_JOIN_STREAM_STREAM_TOPIC} topic.
     *
     * <p>An inner join emits an output when both streams have records with the same key, i.e. when an order is
     * delivered within the join window.
     *
     * <p>{@link JoinWindows} are aligned to the record's timestamp. They are created each time a record is processed
     * and are bounded as [timestamp - before, timestamp + after]. An output is produced if a record from the secondary
     * stream has a timestamp within the window of a record from the primary stream, such as:
     *
     * <pre>
     * {@code stream1.ts - before <= stream2.ts AND stream2.ts <= stream1.ts + after}
     * </pre>
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        KStream<String, Delivery> deliveryStream = streamsBuilder.<String, Delivery>stream(
                        DELIVERY_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, delivery) -> log.info("Processing key = {}, value = {}", key, delivery));

        streamsBuilder.<String, Order>stream(ORDER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, order) -> log.info("Processing key = {}, value = {}", key, order))
                .join(
                        deliveryStream,
                        (key, order, delivery) -> {
                            log.info(
                                    "Joined order {} to delivery {} by order id {}",
                                    order.getId(),
                                    delivery.getId(),
                                    key);

                            return JoinOrderDelivery.newBuilder()
                                    .setOrder(order)
                                    .setDelivery(delivery)
                                    .build();
                        },
                        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1)),
                        StreamJoined.<String, Order, Delivery>with(
                                        Serdes.String(), SerdesUtils.getValueSerdes(), SerdesUtils.getValueSerdes())
                                .withStoreName(ORDER_DELIVERY_JOIN_STREAM_STREAM_STORE))
                .to(
                        ORDER_DELIVERY_JOIN_STREAM_STREAM_TOPIC,
                        Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
