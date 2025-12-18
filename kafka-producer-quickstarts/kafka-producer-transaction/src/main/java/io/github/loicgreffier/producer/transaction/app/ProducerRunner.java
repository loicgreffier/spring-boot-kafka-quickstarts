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

import static io.github.loicgreffier.producer.transaction.constant.Topic.FIRST_STRING_TOPIC;
import static io.github.loicgreffier.producer.transaction.constant.Topic.SECOND_STRING_TOPIC;

import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
    private final Producer<String, String> producer;

    @Setter
    private boolean stopped = false;

    /**
     * Constructor.
     *
     * @param producer The Kafka producer
     */
    public ProducerRunner(Producer<String, String> producer) {
        this.producer = producer;
    }

    /**
     * Asynchronously starts the Kafka producer when the application is ready.
     *
     * <p>The {@code @Async} annotation is used to run the producer in a separate thread, ensuring it does not block the
     * main thread.
     *
     * <p>The Kafka producer sends two string records to two topics: {@code FIRST_STRING_TOPIC} and
     * {@code SECOND_STRING_TOPIC}, within a single transaction. Either both records are successfully committed as part
     * of the transaction, or both are discarded if the transaction fails.
     *
     * @throws InterruptedException if the thread is interrupted while sleeping
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() throws InterruptedException {
        log.info("Init transactions");
        producer.initTransactions();

        int i = 0;
        while (!stopped) {
            ProducerRecord<String, String> firstMessage =
                    new ProducerRecord<>(FIRST_STRING_TOPIC, String.valueOf(i), "Message %s".formatted(i));

            ProducerRecord<String, String> secondMessage =
                    new ProducerRecord<>(SECOND_STRING_TOPIC, String.valueOf(i), "Message %s".formatted(i));

            List<ProducerRecord<String, String>> messages = List.of(firstMessage, secondMessage);

            try {
                log.info("Begin transaction");
                producer.beginTransaction();

                messages.forEach(message -> producer.send(message, (recordMetadata, e) -> {
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
                }));

                if (Integer.parseInt(messages.getFirst().key()) % 3 == 0) {
                    throw new Exception("Error during transaction...");
                }

                log.info("Commit transaction");
                producer.commitTransaction();
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException _) {
                log.info("Closing producer");
                producer.close();
            } catch (Exception e) {
                log.info("Abort transaction", e);
                producer.abortTransaction();
            }

            TimeUnit.SECONDS.sleep(1);

            i++;
        }
    }
}
