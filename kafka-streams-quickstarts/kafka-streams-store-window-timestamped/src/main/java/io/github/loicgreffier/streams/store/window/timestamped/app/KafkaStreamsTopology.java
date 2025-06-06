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
package io.github.loicgreffier.streams.store.window.timestamped.app;

import static io.github.loicgreffier.streams.store.window.timestamped.constant.StateStore.USER_TIMESTAMPED_WINDOW_STORE;
import static io.github.loicgreffier.streams.store.window.timestamped.constant.StateStore.USER_TIMESTAMPED_WINDOW_SUPPLIER_STORE;
import static io.github.loicgreffier.streams.store.window.timestamped.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.store.window.timestamped.app.processor.PutInStoreProcessor;
import io.github.loicgreffier.streams.store.window.timestamped.serdes.SerdesUtils;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the {@code USER_TOPIC} and processes the records using
     * the {@link PutInStoreProcessor} processor, which writes the records to a {@link TimestampedWindowStore} state
     * store. It demonstrates two strategies for using a state store in a processor:
     *
     * <ul>
     *   <li>Using {@link StreamsBuilder#addStateStore(StoreBuilder)} and specifying the store names in the
     *       {@link org.apache.kafka.streams.kstream.KStream#process(ProcessorSupplier, String...)} method.
     *   <li>Using the {@link ProcessorSupplier#stores()} method to attach the store to both the topology and the
     *       processor.
     * </ul>
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        final StoreBuilder<TimestampedWindowStore<String, KafkaUser>> storeBuilder =
                Stores.timestampedWindowStoreBuilder(
                        Stores.persistentTimestampedWindowStore(
                                USER_TIMESTAMPED_WINDOW_STORE, Duration.ofMinutes(10), Duration.ofMinutes(5), false),
                        Serdes.String(),
                        SerdesUtils.getValueSerdes());

        streamsBuilder.addStateStore(storeBuilder).<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .process(() -> new PutInStoreProcessor(storeBuilder.name()), storeBuilder.name());

        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .process(new ProcessorSupplier<String, KafkaUser, String, KafkaUser>() {
                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        StoreBuilder<TimestampedWindowStore<String, KafkaUser>> supplierStoreBuilder =
                                Stores.timestampedWindowStoreBuilder(
                                        Stores.persistentTimestampedWindowStore(
                                                USER_TIMESTAMPED_WINDOW_SUPPLIER_STORE,
                                                Duration.ofMinutes(10),
                                                Duration.ofMinutes(5),
                                                false),
                                        Serdes.String(),
                                        SerdesUtils.getValueSerdes());

                        return Collections.singleton(supplierStoreBuilder);
                    }

                    @Override
                    public Processor<String, KafkaUser, String, KafkaUser> get() {
                        return new PutInStoreProcessor(USER_TIMESTAMPED_WINDOW_SUPPLIER_STORE);
                    }
                });
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
