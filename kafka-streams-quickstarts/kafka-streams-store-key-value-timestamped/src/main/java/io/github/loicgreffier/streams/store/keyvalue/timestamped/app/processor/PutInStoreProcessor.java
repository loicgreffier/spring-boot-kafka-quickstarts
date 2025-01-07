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

package io.github.loicgreffier.streams.store.keyvalue.timestamped.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * This class represents a processor that puts the messages in a timestamped key-value store.
 */
@Slf4j
public class PutInStoreProcessor extends ContextualProcessor<String, KafkaPerson, String, KafkaPerson> {
    private final String storeName;
    private TimestampedKeyValueStore<String, KafkaPerson> timestampedKeyValueStore;

    /**
     * Constructor.
     *
     * @param storeName the name of the store.
     */
    public PutInStoreProcessor(String storeName) {
        this.storeName = storeName;
    }

    /**
     * Initialize the processor.
     *
     * @param context the processor context.
     */
    @Override
    public void init(ProcessorContext<String, KafkaPerson> context) {
        super.init(context);
        timestampedKeyValueStore = context.getStateStore(storeName);
    }

    /**
     * Inserts the message in the state store.
     *
     * @param message the message to process.
     */
    @Override
    public void process(Record<String, KafkaPerson> message) {
        log.info("Put key = {}, value = {} in store {}", message.key(), message.value(), storeName);
        timestampedKeyValueStore.put(message.key(), ValueAndTimestamp.make(message.value(), message.timestamp()));
    }
}
