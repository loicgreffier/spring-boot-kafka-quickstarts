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

import static io.github.loicgreffier.streams.join.stream.globaltable.constant.StateStore.CUSTOMER_STORE;
import static io.github.loicgreffier.streams.join.stream.globaltable.constant.Topic.CUSTOMER_TOPIC;
import static io.github.loicgreffier.streams.join.stream.globaltable.constant.Topic.ORDER_CUSTOMER_JOIN_STREAM_GLOBAL_TABLE_TOPIC;
import static io.github.loicgreffier.streams.join.stream.globaltable.constant.Topic.ORDER_TOPIC;

import io.github.loicgreffier.avro.Customer;
import io.github.loicgreffier.avro.JoinOrderCustomer;
import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.streams.join.stream.globaltable.serdes.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
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
     * <p>This topology reads from the {@code ORDER_TOPIC} topic and the {@code CUSTOMER_TOPIC} topic as a global table.
     * The stream is joined to the global table by customer id using an inner join. The customer id is extracted from
     * the order value by the key mapper, so the stream does not need to be re-keyed. The result is written to the
     * {@code ORDER_CUSTOMER_JOIN_STREAM_GLOBAL_TABLE_TOPIC} topic.
     *
     * <p>An inner join emits an output record only when the global table has a customer matching the customer id of the
     * order.
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        GlobalKTable<String, Customer> customerGlobalTable = streamsBuilder.globalTable(
                CUSTOMER_TOPIC,
                Materialized.<String, Customer, KeyValueStore<Bytes, byte[]>>as(CUSTOMER_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SerdesUtils.getValueSerdes()));

        streamsBuilder.<String, Order>stream(ORDER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, order) -> log.info("Processing key = {}, value = {}", key, order))
                .join(customerGlobalTable, (_, order) -> String.valueOf(order.getCustomerId()), (order, customer) -> {
                    log.info(
                            "Joined order {} to customer {} {} by customer id {}",
                            order.getId(),
                            customer.getFirstName(),
                            customer.getLastName(),
                            order.getCustomerId());

                    return JoinOrderCustomer.newBuilder()
                            .setOrder(order)
                            .setCustomer(customer)
                            .build();
                })
                .to(
                        ORDER_CUSTOMER_JOIN_STREAM_GLOBAL_TABLE_TOPIC,
                        Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
