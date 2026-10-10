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
package io.github.loicgreffier.streams.schedule.app.processor;

import static io.github.loicgreffier.streams.schedule.constant.StateStore.ORDER_SCHEDULE_STORE;

import io.github.loicgreffier.avro.Order;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class represents a processor that counts the number of orders by customer. */
public class CountOrderProcessor extends ContextualProcessor<String, Order, String, Long> {
    private static final Logger log = LoggerFactory.getLogger(CountOrderProcessor.class);
    private KeyValueStore<String, Long> countOrderStore;

    /**
     * Initialize the processor. Opens the state store and schedules the punctuations. The first punctuation is
     * scheduled on wall clock time and resets the counters every 2 minutes. The second punctuation is scheduled on
     * stream time and forwards the counters to the output topic every minute. The stream time avoids triggering
     * unnecessary punctuation if no record comes.
     *
     * @param context The processor context.
     */
    @Override
    public void init(ProcessorContext<String, Long> context) {
        super.init(context);
        countOrderStore = context.getStateStore(ORDER_SCHEDULE_STORE);
        context.schedule(Duration.ofMinutes(2), PunctuationType.WALL_CLOCK_TIME, this::resetCounters);
        context.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, this::forwardCounters);
    }

    /**
     * For each message processed, increments the counter of the corresponding customer in the state store.
     *
     * @param message The message to process.
     */
    @Override
    public void process(Record<String, Order> message) {
        log.atInfo().addArgument(message.key()).addArgument(message.value()).log("Processing key = {}, value = {}");

        String key = String.valueOf(message.value().getCustomerId());
        Long count = countOrderStore.putIfAbsent(key, 1L);

        if (count != null) {
            countOrderStore.put(key, count + 1);
        }
    }

    /**
     * For each entry in the state store, resets the counter to 0.
     *
     * @param timestamp The timestamp of the punctuation.
     */
    private void resetCounters(long timestamp) {
        try (KeyValueIterator<String, Long> iterator = countOrderStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, Long> keyValue = iterator.next();
                countOrderStore.put(keyValue.key, 0L);
            }
        }

        log.atInfo()
                .addArgument(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                        .withZone(ZoneId.systemDefault())
                        .format(Instant.ofEpochMilli(timestamp)))
                .log("All counters reset at {}");
    }

    /**
     * For each entry in the state store, forwards the counter to the output topic.
     *
     * @param timestamp The timestamp of the punctuation.
     */
    private void forwardCounters(long timestamp) {
        try (KeyValueIterator<String, Long> iterator = countOrderStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, Long> keyValue = iterator.next();
                context().forward(new Record<>(keyValue.key, keyValue.value, timestamp));
                log.atInfo()
                        .addArgument(keyValue.value)
                        .addArgument(keyValue.key)
                        .addArgument(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                                .withZone(ZoneId.systemDefault())
                                .format(Instant.ofEpochMilli(timestamp)))
                        .log("{} orders of customer {} at {}");
            }
        }
    }
}
