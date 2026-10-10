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
package io.github.loicgreffier.consumer.headers.app;

import static io.github.loicgreffier.consumer.headers.constant.Topic.ORDER_JSON_TOPIC;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/** This class represents a Kafka consumer runner that subscribes to a specific topic and processes Kafka records. */
@Component
public class ConsumerRunner {
    private static final Logger log = LoggerFactory.getLogger(ConsumerRunner.class);
    private final Consumer<String, String> consumer;

    /**
     * Constructor.
     *
     * @param consumer The Kafka consumer.
     */
    public ConsumerRunner(Consumer<String, String> consumer) {
        this.consumer = consumer;
    }

    /**
     * Asynchronously starts the Kafka consumer when the application is ready.
     *
     * <p>The {@code @Async} annotation ensures that the consumer runs in a separate thread, preventing it from blocking
     * the main application thread during startup.
     *
     * <p>This Kafka consumer listens to the {@code ORDER_JSON_TOPIC} and processes orders serialized as JSON strings,
     * along with their {@code correlationId} and {@code eventType} headers.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            log.info("Subscribing to {} topic", ORDER_JSON_TOPIC);

            consumer.subscribe(Collections.singleton(ORDER_JSON_TOPIC), new CustomConsumerRebalanceListener());

            while (true) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
                log.info("Pulled {} records", messages.count());

                long startTime = System.currentTimeMillis();

                for (ConsumerRecord<String, String> message : messages) {
                    Header correlationId = message.headers().lastHeader("correlationId");
                    String correlationIdValue =
                            correlationId != null ? new String(correlationId.value(), StandardCharsets.UTF_8) : "";

                    Header eventType = message.headers().lastHeader("eventType");
                    String eventTypeValue =
                            eventType != null ? new String(eventType.value(), StandardCharsets.UTF_8) : "";

                    log.info(
                            "Processing offset = {}, partition = {}, key = {}, value = {}, correlationId = {}, "
                                    + "eventType = {}",
                            message.offset(),
                            message.partition(),
                            message.key(),
                            message.value(),
                            correlationIdValue,
                            eventTypeValue);
                }

                long processingTimeMs = System.currentTimeMillis() - startTime;
                log.info("Processing {} records took {} ms", messages.count(), processingTimeMs);

                if (!messages.isEmpty()) {
                    doCommitSync();
                }
            }
        } catch (WakeupException _) {
            log.info("Wake up signal received");
        } finally {
            log.info("Closing consumer");
            consumer.close();
        }
    }

    /** Performs a synchronous commit of the consumed records. */
    private void doCommitSync() {
        try {
            log.info("Committing the pulled records");
            consumer.commitSync();
        } catch (WakeupException e) {
            log.info("Wake up signal received during commit process");
            doCommitSync();
            throw e;
        } catch (CommitFailedException e) {
            log.warn("Failed to commit", e);
        }
    }
}
