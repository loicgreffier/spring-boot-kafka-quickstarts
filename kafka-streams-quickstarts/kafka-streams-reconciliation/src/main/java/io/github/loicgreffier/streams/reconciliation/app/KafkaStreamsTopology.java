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
import static io.github.loicgreffier.streams.reconciliation.constant.Topic.RECONCILIATION_TOPIC;
import static io.github.loicgreffier.streams.reconciliation.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaOrder;
import io.github.loicgreffier.avro.KafkaReconciliation;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.reconciliation.app.processor.ReconciliationProcessor;
import io.github.loicgreffier.streams.reconciliation.serdes.SerdesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the {@code USER_TOPIC} and {@code ORDER_TOPIC} topics.
     * It reconciles a customer and an order, regardless of which record arrives first or how much time passes between
     * the two. The {@code ORDER_TOPIC} is repartitioned before entering the processor to prevent records with a new key
     * from being processed by the wrong task. The result is written to the {@code RECONCILIATION_TOPIC}.
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        StoreBuilder<KeyValueStore<String, KafkaReconciliation>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(RECONCILIATION_STORE), Serdes.String(), SerdesUtils.getValueSerdes());

        streamsBuilder.addStateStore(storeBuilder);

        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .process(() -> new ReconciliationProcessor<>(), RECONCILIATION_STORE)
                .to(RECONCILIATION_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));

        streamsBuilder.<String, KafkaOrder>stream(
                        ORDER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .selectKey((key, value) -> String.valueOf(value.getCustomerId()))
                .repartition(Repartitioned.<String, KafkaOrder>with(Serdes.String(), SerdesUtils.getValueSerdes())
                        .withName(ORDER_TOPIC))
                .process(() -> new ReconciliationProcessor<>(), RECONCILIATION_STORE)
                .to(RECONCILIATION_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
