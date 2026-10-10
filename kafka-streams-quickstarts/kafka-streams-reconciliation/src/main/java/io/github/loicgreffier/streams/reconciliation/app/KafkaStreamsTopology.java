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
package io.github.loicgreffier.streams.reconciliation.app;

import static io.github.loicgreffier.streams.reconciliation.constant.StateStore.RECONCILIATION_STORE;
import static io.github.loicgreffier.streams.reconciliation.constant.Topic.ORDER_TOPIC;
import static io.github.loicgreffier.streams.reconciliation.constant.Topic.PAYMENT_TOPIC;
import static io.github.loicgreffier.streams.reconciliation.constant.Topic.RECONCILIATION_TOPIC;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.Payment;
import io.github.loicgreffier.avro.Reconciliation;
import io.github.loicgreffier.streams.reconciliation.app.processor.ReconciliationProcessor;
import io.github.loicgreffier.streams.reconciliation.serdes.SerdesUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/** Kafka Streams topology. */
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     *
     * <p>This topology reads from the {@code ORDER_TOPIC} and {@code PAYMENT_TOPIC} topics. It reconciles an order and
     * its payment, regardless of which record arrives first or how much time passes between the two. Both topics are
     * keyed by order id, so they are co-partitioned: an order and its payment are processed by the same task and share
     * the same reconciliation store. The result is written to the {@code RECONCILIATION_TOPIC}.
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        StoreBuilder<KeyValueStore<String, Reconciliation>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(RECONCILIATION_STORE), Serdes.String(), SerdesUtils.getValueSerdes());

        streamsBuilder.addStateStore(storeBuilder);

        streamsBuilder.<String, Order>stream(ORDER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .process(() -> new ReconciliationProcessor<>(), RECONCILIATION_STORE)
                .to(RECONCILIATION_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));

        streamsBuilder.<String, Payment>stream(
                        PAYMENT_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .process(() -> new ReconciliationProcessor<>(), RECONCILIATION_STORE)
                .to(RECONCILIATION_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
