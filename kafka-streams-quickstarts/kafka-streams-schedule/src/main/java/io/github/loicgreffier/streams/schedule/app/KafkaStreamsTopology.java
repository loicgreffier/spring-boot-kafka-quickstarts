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

package io.github.loicgreffier.streams.schedule.app;

import static io.github.loicgreffier.streams.schedule.constant.StateStore.USER_SCHEDULE_STORE;
import static io.github.loicgreffier.streams.schedule.constant.Topic.USER_SCHEDULE_TOPIC;
import static io.github.loicgreffier.streams.schedule.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.schedule.app.processor.CountNationalityProcessor;
import io.github.loicgreffier.streams.schedule.serdes.SerdesUtils;
import java.util.Collections;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the USER_TOPIC topic and processes the records with
     * the {@link CountNationalityProcessor} processor. The processor supplier registers
     * a {@link KeyValueStore} state store when it is built.
     * The result is written to the USER_SCHEDULE_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaUser>stream(USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .process(new ProcessorSupplier<String, KafkaUser, String, Long>() {
                @Override
                public Set<StoreBuilder<?>> stores() {
                    StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores
                        .keyValueStoreBuilder(
                            Stores.persistentKeyValueStore(USER_SCHEDULE_STORE),
                            Serdes.String(), Serdes.Long()
                        );

                    return Collections.singleton(storeBuilder);
                }

                @Override
                public Processor<String, KafkaUser, String, Long> get() {
                    return new CountNationalityProcessor();
                }
            })
            .to(USER_SCHEDULE_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }
}
