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
package io.github.loicgreffier.consumer.transaction.app;

import static io.github.loicgreffier.consumer.transaction.constant.Topic.FIRST_STRING_TOPIC;
import static io.github.loicgreffier.consumer.transaction.constant.Topic.SECOND_STRING_TOPIC;

import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
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
     * Asynchronously starts the Kafka consumer when the application is ready.
     *
     * <p>The {@code @Async} annotation ensures that the consumer runs in a separate thread and does not block the main
     * application thread.
     *
     * <p>This Kafka consumer processes string records from the {@code FIRST_STRING_TOPIC} and
     * {@code SECOND_STRING_TOPIC} topics with an isolation level of {@code read_committed}, meaning it will only read
     * records that are part of a committed transaction or records not associated with any transaction.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            log.info("Subscribing to {} and {} topics", FIRST_STRING_TOPIC, SECOND_STRING_TOPIC);

            consumer.subscribe(List.of(FIRST_STRING_TOPIC, SECOND_STRING_TOPIC), new CustomConsumerRebalanceListener());

            while (true) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
                log.info("Pulled {} records", messages.count());

                for (ConsumerRecord<String, String> message : messages) {
                    log.info(
                            "Received offset = {}, partition = {}, key = {}, value = {}",
                            message.offset(),
                            message.partition(),
                            message.key(),
                            message.value());
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
