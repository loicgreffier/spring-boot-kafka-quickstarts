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

package io.github.loicgreffier.streams.store.cleanup.app.processor;

import static io.github.loicgreffier.streams.store.cleanup.constant.StateStore.PERSON_SCHEDULE_STORE_CLEANUP_STORE;

import io.github.loicgreffier.avro.KafkaPerson;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * This class represents a processor that fills and cleans a state store.
 */
@Slf4j
public class StoreCleanupProcessor extends ContextualProcessor<String, KafkaPerson, String, KafkaPerson> {
    private KeyValueStore<String, KafkaPerson> personStore;

    /**
     * Initialize the processor.
     * Opens the state store and schedules the punctuation. The punctuation is scheduled on stream time and forwards
     * the tombstones. The stream time is used to avoid triggering unnecessary punctuation if no record comes.
     *
     * @param context the processor context.
     */
    @Override
    public void init(ProcessorContext<String, KafkaPerson> context) {
        super.init(context);
        personStore = context.getStateStore(PERSON_SCHEDULE_STORE_CLEANUP_STORE);
        context.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, this::forwardTombstones);
    }

    /**
     * For each message processed, puts the message in the state store. If the message is a tombstone, deletes the
     * corresponding key from the state store.
     *
     * @param message the message to process.
     */
    @Override
    public void process(Record<String, KafkaPerson> message) {
        if (message.value() == null) {
            log.info("Received tombstone for key = {}", message.key());
            personStore.delete(message.key());
            return;
        }

        log.info("Received key = {}, value = {}", message.key(), message.value());
        personStore.put(message.key(), message.value());
    }

    /**
     * For each entry in the state store, forwards a tombstone to the output topic
     * (which is also the input topic of the processor).
     * When the tombstone is received by the processor, the corresponding key is deleted.
     *
     * @param timestamp the timestamp of the punctuation.
     */
    private void forwardTombstones(long timestamp) {
        log.info("Resetting {} store ", PERSON_SCHEDULE_STORE_CLEANUP_STORE);

        try (KeyValueIterator<String, KafkaPerson> iterator = personStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, KafkaPerson> keyValue = iterator.next();
                context().forward(new Record<>(keyValue.key, null, timestamp));
            }
        }
    }
}
