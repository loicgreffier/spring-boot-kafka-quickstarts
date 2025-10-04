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
package io.github.loicgreffier.consumer.exactly.once.app;

import static io.github.loicgreffier.consumer.exactly.once.constant.Topic.EXACTLY_ONCE_PROCESSING_TOPIC;
import static io.github.loicgreffier.consumer.exactly.once.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import java.time.Duration;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * This class represents a Kafka consumer runner that subscribes to a specific topic, processes Kafka records, and sends
 * the transformed records back to a topic.
 */
@Slf4j
@Component
public class ConsumerRunner {
    private final Consumer<String, KafkaUser> consumer;
    private final Producer<String, KafkaUser> producer;

    /**
     * Constructor.
     *
     * @param consumer The Kafka consumer.
     * @param producer The Kafka producer.
     */
    public ConsumerRunner(Consumer<String, KafkaUser> consumer, Producer<String, KafkaUser> producer) {
        this.consumer = consumer;
        this.producer = producer;
    }

    /**
     * Asynchronously starts the Kafka consumer when the application is ready.
     *
     * <p>The {@code @Async} annotation is used to run the consumer in a separate thread, ensuring that it does not
     * block the main application thread during startup.
     *
     * <p>This Kafka consumer listens to the {@code USER_TOPIC}, and processes records by mapping the first name and
     * last name to uppercase. It then sends the transformed records to the {@code EXACTLY_ONCE_PROCESSING_TOPIC} using
     * transactions, forming a consume-process-produce loop.
     *
     * <p>Transactions ensure that processed records are sent to the output topic along with the offsets of the
     * partitions that were processed. This guarantees exactly-once processing, meaning that for each record received,
     * its processed results will be reflected once, even in the event of failures. Without transactions, processing
     * could lead to duplicate records in the output topic if a failure occurs after sending the records but before
     * committing the offsets.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            log.info("Subscribing to {} topic", USER_TOPIC);
            consumer.subscribe(Collections.singleton(USER_TOPIC), new CustomConsumerRebalanceListener());

            log.info("Init transactions");
            producer.initTransactions();

            while (true) {
                ConsumerRecords<String, KafkaUser> messages = consumer.poll(Duration.ofMillis(1000));
                log.info("Pulled {} records", messages.count());

                if (!messages.isEmpty()) {
                    log.info("Begin transaction");
                    producer.beginTransaction();

                    List<ProducerRecord<String, KafkaUser>> transformedMessages = messages.partitions().stream()
                            .flatMap(partition -> messages.records(partition).stream()
                                    .map(message -> {
                                        log.info(
                                                "Received offset = {}, partition = {}, key = {}, value = {}",
                                                message.offset(),
                                                message.partition(),
                                                message.key(),
                                                message.value());

                                        KafkaUser kafkaUser = message.value();
                                        kafkaUser.setFirstName(
                                                kafkaUser.getFirstName().toUpperCase());
                                        kafkaUser.setLastName(
                                                kafkaUser.getLastName().toUpperCase());
                                        return new ProducerRecord<>(
                                                EXACTLY_ONCE_PROCESSING_TOPIC, message.key(), kafkaUser);
                                    }))
                            .toList();

                    transformedMessages.forEach(
                            transformedMessage -> producer.send(transformedMessage, (recordMetadata, e) -> {
                                if (e != null) {
                                    log.error(e.getMessage());
                                } else {
                                    log.info(
                                            "Success: topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                                            recordMetadata.topic(),
                                            recordMetadata.partition(),
                                            recordMetadata.offset(),
                                            transformedMessage.key(),
                                            transformedMessage.value());
                                }
                            }));

                    producer.sendOffsetsToTransaction(offsetsToCommit(), consumer.groupMetadata());

                    log.info("Commit transaction");
                    producer.commitTransaction();
                }
            }
        } catch (WakeupException _) {
            log.info("Wake up signal received");
        } catch (Exception e) {
            log.info("Abort transaction", e);
            producer.abortTransaction();
        } finally {
            log.info("Closing consumer");
            consumer.close();
        }
    }

    /**
     * Retrieves the offsets to commit for the current consumer's assignment.
     *
     * @return A map of topic partitions to their corresponding offsets and metadata.
     */
    private Map<TopicPartition, OffsetAndMetadata> offsetsToCommit() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        for (TopicPartition partition : consumer.assignment()) {
            offsets.put(partition, new OffsetAndMetadata(consumer.position(partition)));
        }

        return offsets;
    }
}
