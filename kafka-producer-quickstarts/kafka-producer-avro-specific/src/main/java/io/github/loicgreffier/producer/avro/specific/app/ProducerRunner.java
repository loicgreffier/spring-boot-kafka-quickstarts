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
package io.github.loicgreffier.producer.avro.specific.app;

import static io.github.loicgreffier.producer.avro.specific.constant.Name.FIRST_NAMES;
import static io.github.loicgreffier.producer.avro.specific.constant.Name.LAST_NAMES;
import static io.github.loicgreffier.producer.avro.specific.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/** This class represents a Kafka producer runner that sends records to a specific topic. */
@Slf4j
@Component
public class ProducerRunner {
    private Random random = new Random();
    private final Producer<String, KafkaUser> producer;

    /**
     * Constructor.
     *
     * @param producer The Kafka producer
     */
    public ProducerRunner(Producer<String, KafkaUser> producer) {
        this.producer = producer;
    }

    /**
     * Asynchronously starts the Kafka producer when the application is ready. The asynchronous annotation is used to
     * run the producer in a separate thread and not block the main thread. The Kafka producer produces specific Avro
     * records to the USER_TOPIC topic.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        int i = 0;
        while (true) {
            ProducerRecord<String, KafkaUser> message =
                    new ProducerRecord<>(USER_TOPIC, String.valueOf(i), buildKafkaUser(i));

            send(message);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                log.error("Interruption during sleep between message production", e);
                Thread.currentThread().interrupt();
            }

            i++;
        }
    }

    /**
     * Sends a message to the Kafka topic.
     *
     * @param message The message to send.
     * @return A future of the record metadata.
     */
    public Future<RecordMetadata> send(ProducerRecord<String, KafkaUser> message) {
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
     * Builds a specific Avro record.
     *
     * @param id The record id.
     * @return The specific Avro record.
     */
    private KafkaUser buildKafkaUser(int id) {
        return KafkaUser.newBuilder()
                .setId((long) id)
                .setFirstName(FIRST_NAMES[random.nextInt(FIRST_NAMES.length)])
                .setLastName(LAST_NAMES[random.nextInt(LAST_NAMES.length)])
                .setBirthDate(Instant.now())
                .build();
    }
}
