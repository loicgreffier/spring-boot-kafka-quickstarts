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
package io.github.loicgreffier.consumer.retry.app;

import static io.github.loicgreffier.consumer.retry.constant.Topic.STRING_TOPIC;
import static org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.EARLIEST;
import static org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy.LATEST;

import io.github.loicgreffier.consumer.retry.property.ConsumerProperties;
import io.github.loicgreffier.consumer.retry.service.ExternalService;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/** This class represents a Kafka consumer runner that subscribes to a specific topic and processes Kafka records. */
@Slf4j
@Component
public class ConsumerRunner {
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    private final Consumer<String, String> consumer;
    private final ExternalService externalService;
    private final ConsumerProperties properties;

    /**
     * Constructor.
     *
     * @param consumer The Kafka consumer.
     * @param externalService The external service.
     * @param properties The consumer properties.
     */
    public ConsumerRunner(
            Consumer<String, String> consumer, ExternalService externalService, ConsumerProperties properties) {
        this.consumer = consumer;
        this.externalService = externalService;
        this.properties = properties;
    }

    /**
     * Asynchronously starts the Kafka consumer when the application is ready.
     *
     * <p>The {@code @Async} annotation ensures that the consumer runs in a separate thread, allowing the main
     * application thread to remain unblocked.
     *
     * <p>This Kafka consumer processes messages from the {@code STRING_TOPIC} topic. If an error occurs during an
     * external system call, the consumer will:
     *
     * <ul>
     *   <li>Pause the affected topic-partitions.
     *   <li>Rewind to the offset of the failed record, since {@code poll()} automatically advances the offsets.
     *   <li>Exit the loop without updating the offset position
     *   <li>Avoid committing offsets while paused, ensuring the failed record can be retried.
     * </ul>
     *
     * <p>While the consumer is paused, {@code poll()} will return no records but will still honor its timeout. After
     * the pause duration elapses, the consumer resumes the topic-partitions and reprocesses the failed record.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            log.info("Subscribing to {} topic", STRING_TOPIC);

            consumer.subscribe(
                    Collections.singleton(STRING_TOPIC), new CustomConsumerRebalanceListener(consumer, offsets));

            while (true) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
                log.info("Pulled {} records", messages.count());

                if (isPaused()) {
                    log.info("Consumer was paused, resuming topic-partitions {}", consumer.assignment());
                    consumer.resume(consumer.assignment());
                }

                for (ConsumerRecord<String, String> message : messages) {
                    log.info(
                            "Received offset = {}, partition = {}, key = {}, value = {}",
                            message.offset(),
                            message.partition(),
                            message.key(),
                            message.value());

                    try {
                        // e.g. enrichment of a record with webservice, database...
                        externalService.call(message);
                    } catch (Exception e) {
                        log.error("Error during external system call", e);

                        log.info("Pausing topic-partitions {}", consumer.assignment());
                        consumer.pause(consumer.assignment());
                        rewind();
                        break;
                    }

                    updateOffsetsPosition(message);
                }

                if (!messages.isEmpty() && !isPaused()) {
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

    /**
     * Checks if the consumer is paused.
     *
     * @return True if the consumer is paused, false otherwise.
     */
    private boolean isPaused() {
        return !consumer.paused().isEmpty();
    }

    /** Rewinds the consumer the last saved offsets for each topic-partition. */
    private void rewind() {
        if (offsets.isEmpty()) {
            String autoOffsetReset = properties
                    .getProperties()
                    .getOrDefault(
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                            EARLIEST.name().toLowerCase());

            if (autoOffsetReset.equalsIgnoreCase(EARLIEST.name().toLowerCase())) {
                consumer.seekToBeginning(consumer.assignment());
            } else if (autoOffsetReset.equalsIgnoreCase(LATEST.name().toLowerCase())) {
                consumer.seekToEnd(consumer.assignment());
            }
        }

        log.info("Seeking to following partitions-offsets {}", offsets);

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            if (entry.getValue() != null) {
                consumer.seek(entry.getKey(), entry.getValue());
            } else {
                log.warn(
                        "Cannot rewind on {} to null offset, this could happen if the consumer "
                                + "group was just created",
                        entry.getKey());
            }
        }
    }

    /**
     * Saves the offset of a successfully processed record by topic-partition.
     *
     * <p>This method stores the offset of the next record to be processed. In the event of an external system failure,
     * this allows the consumer to rewind and reprocess the last failed record by seeking to the previously saved
     * offset.
     *
     * @param message The message that was successfully processed.
     */
    private void updateOffsetsPosition(ConsumerRecord<String, String> message) {
        offsets.put(
                new TopicPartition(message.topic(), message.partition()), new OffsetAndMetadata(message.offset() + 1));
    }

    /** Performs a synchronous commit of the consumed records. */
    private void doCommitSync() {
        try {
            log.info("Committing the pulled records");
            consumer.commitSync(offsets);
        } catch (WakeupException e) {
            log.info("Wake up signal received during commit process");
            doCommitSync();
            throw e;
        } catch (CommitFailedException e) {
            log.warn("Failed to commit", e);
        }
    }
}
