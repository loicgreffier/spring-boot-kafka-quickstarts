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

import static io.github.loicgreffier.consumer.headers.constant.Topic.STRING_TOPIC;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/** This class represents a Kafka consumer runner that subscribes to a specific topic and processes Kafka records. */
@Slf4j
@Component
public class ConsumerRunner {
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
     * Asynchronously starts the Kafka consumer once the application is ready. The {@code @Async} annotation ensures
     * that the consumer runs in a separate thread, preventing the main thread from being blocked. The Kafka consumer
     * processes string records with headers from the STRING_TOPIC topic.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            log.info("Subscribing to {} topic", STRING_TOPIC);

            consumer.subscribe(Collections.singleton(STRING_TOPIC), new CustomConsumerRebalanceListener());

            while (true) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
                log.info("Pulled {} records", messages.count());

                for (ConsumerRecord<String, String> message : messages) {
                    Header headerMessage = message.headers().lastHeader("message");
                    String headerMessageValue =
                            headerMessage != null ? new String(headerMessage.value(), StandardCharsets.UTF_8) : "";

                    log.info(
                            "Received offset = {}, partition = {}, key = {}, value = {}, header = {}",
                            message.offset(),
                            message.partition(),
                            message.key(),
                            message.value(),
                            headerMessageValue);
                }

                if (!messages.isEmpty()) {
                    doCommitSync();
                }
            }
        } catch (WakeupException e) {
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
