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
package io.github.loicgreffier.producer.transaction.app;

import static io.github.loicgreffier.producer.transaction.constant.Name.FIRST_NAMES;
import static io.github.loicgreffier.producer.transaction.constant.Name.LAST_NAMES;
import static io.github.loicgreffier.producer.transaction.constant.Topic.FIRST_STRING_TOPIC;
import static io.github.loicgreffier.producer.transaction.constant.Topic.SECOND_STRING_TOPIC;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/** This class represents a Kafka producer runner that sends records to a specific topic. */
@Slf4j
@Component
public class ProducerRunner {
    private final Random random = new Random();
    private final Producer<String, String> producer;

    /**
     * Constructor.
     *
     * @param producer The Kafka producer
     */
    public ProducerRunner(Producer<String, String> producer) {
        this.producer = producer;
    }

    /**
     * Asynchronously starts the Kafka producer when the application is ready. The asynchronous annotation is used to
     * run the producer in a separate thread and not block the main thread. The Kafka producer produces two string
     * records to two topics (FIRST_STRING_TOPIC and SECOND_STRING_TOPIC) in a single transaction. Either both records
     * are validated by the transaction or both records are discarded.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        log.info("Init transactions");
        producer.initTransactions();

        while (true) {
            String kafkaUser = buildKafkaUser();
            String key = UUID.nameUUIDFromBytes(kafkaUser.getBytes()).toString();
            String firstName = kafkaUser.split(" ")[0];
            String lastName = kafkaUser.split(" ")[1];

            ProducerRecord<String, String> firstNameMessage = new ProducerRecord<>(FIRST_STRING_TOPIC, key, firstName);

            ProducerRecord<String, String> lastNameMessage = new ProducerRecord<>(SECOND_STRING_TOPIC, key, lastName);

            sendInTransaction(List.of(firstNameMessage, lastNameMessage));

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                log.error("Interruption during sleep between message production", e);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Sends a list of messages to the Kafka topics in a single transaction. If the first message value is greater than
     * or equal to 7, an exception is thrown to simulate an error and abort the transaction.
     *
     * @param messages The messages to send.
     */
    public final void sendInTransaction(List<ProducerRecord<String, String>> messages) {
        try {
            log.info("Begin transaction");
            producer.beginTransaction();

            messages.forEach(this::send);

            if (messages.getFirst().value().length() >= 7) {
                throw new Exception("Error during transaction...");
            }

            log.info("Commit transaction");
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            log.info("Closing producer");
            producer.close();
        } catch (Exception e) {
            log.info("Abort transaction", e);
            producer.abortTransaction();
        }
    }

    /**
     * Sends a message to the Kafka topic.
     *
     * @param message The message to send.
     * @return A future of the record metadata.
     */
    public Future<RecordMetadata> send(ProducerRecord<String, String> message) {
        return producer.send(message, (recordMetadata, e) -> {
            if (e != null) {
                log.error(e.getMessage());
            } else {
                log.info(
                        "Success: topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        message.key(),
                        message.value());
            }
        });
    }

    /**
     * Builds a Kafka user as a string record.
     *
     * @return The string record.
     */
    private String buildKafkaUser() {
        String firstName = FIRST_NAMES[random.nextInt(FIRST_NAMES.length)];
        String lastName = LAST_NAMES[random.nextInt(LAST_NAMES.length)];

        return String.format("%s %s", firstName, lastName);
    }
}
