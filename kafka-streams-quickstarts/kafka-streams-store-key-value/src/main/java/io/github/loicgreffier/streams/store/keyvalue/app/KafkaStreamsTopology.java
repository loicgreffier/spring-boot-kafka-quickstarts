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
package io.github.loicgreffier.streams.store.keyvalue.app;

import static io.github.loicgreffier.streams.store.keyvalue.constant.StateStore.USER_KEY_VALUE_STORE;
import static io.github.loicgreffier.streams.store.keyvalue.constant.StateStore.USER_KEY_VALUE_SUPPLIER_STORE;
import static io.github.loicgreffier.streams.store.keyvalue.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.store.keyvalue.app.processor.PutInStoreProcessor;
import io.github.loicgreffier.streams.store.keyvalue.serdes.SerdesUtils;
import java.util.Collections;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the USER_TOPIC topic and processes the records with
     * the {@link PutInStoreProcessor} processor that puts the records in a {@link KeyValueStore} state store. It
     * demonstrates the two strategies to use a state store in a processor: - Using the
     * {@link StreamsBuilder#addStateStore(StoreBuilder)} and specifying the store names in the
     * {@link org.apache.kafka.streams.kstream.KStream#process(ProcessorSupplier, String...)} method. - Using the
     * {@link ProcessorSupplier#stores()} method to attach the store to the topology and the processor.
     *
     * @param streamsBuilder The streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        final StoreBuilder<KeyValueStore<String, KafkaUser>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(USER_KEY_VALUE_STORE), Serdes.String(), SerdesUtils.getValueSerdes());

        streamsBuilder.addStateStore(storeBuilder).<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .process(() -> new PutInStoreProcessor(storeBuilder.name()), storeBuilder.name());

        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .process(new ProcessorSupplier<String, KafkaUser, String, KafkaUser>() {
                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        StoreBuilder<KeyValueStore<String, KafkaUser>> supplierStoreBuilder =
                                Stores.keyValueStoreBuilder(
                                        Stores.persistentKeyValueStore(USER_KEY_VALUE_SUPPLIER_STORE),
                                        Serdes.String(),
                                        SerdesUtils.getValueSerdes());

                        return Collections.singleton(supplierStoreBuilder);
                    }

                    @Override
                    public Processor<String, KafkaUser, String, KafkaUser> get() {
                        return new PutInStoreProcessor(USER_KEY_VALUE_SUPPLIER_STORE);
                    }
                });
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
