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
package io.github.loicgreffier.streams.producer.user.app;

import static io.github.loicgreffier.streams.producer.user.constant.Name.FIRST_NAMES;
import static io.github.loicgreffier.streams.producer.user.constant.Name.LAST_NAMES;
import static io.github.loicgreffier.streams.producer.user.constant.Topic.USER_TOPIC;
import static io.github.loicgreffier.streams.producer.user.constant.Topic.USER_TOPIC_TWO;

import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaUser;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/** This class represents a Kafka producer runner that sends records to a specific topic. */
@Slf4j
@Component
public class ProducerRunner {
    private final Random random = new Random();
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
     * Asynchronously starts the Kafka producer when the application is ready.
     *
     * <p>The {@code @Async} annotation is used to run the producer in a separate thread, preventing it from blocking
     * the main thread.
     *
     * <p>The Kafka producer sends user records to two topics: {@code USER_TOPIC} and {@code USER_TOPIC_TWO}.
     *
     * @throws InterruptedException if the thread is interrupted while sleeping
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() throws InterruptedException {
        int i = 0;
        while (true) {
            ProducerRecord<String, KafkaUser> messageOne =
                    new ProducerRecord<>(USER_TOPIC, String.valueOf(i), buildKafkaUser(i));

            ProducerRecord<String, KafkaUser> messageTwo =
                    new ProducerRecord<>(USER_TOPIC_TWO, String.valueOf(i), buildKafkaUser(i));

            send(messageOne);
            send(messageTwo);

            TimeUnit.SECONDS.sleep(1);

            i++;
        }
    }

    /**
     * Sends a message to the Kafka topic.
     *
     * @param message The message to send.
     */
    public void send(ProducerRecord<String, KafkaUser> message) {
        producer.send(message, (recordMetadata, e) -> {
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
     * Builds a Kafka user.
     *
     * @param id The user id.
     * @return The Kafka user.
     */
    private KafkaUser buildKafkaUser(int id) {
        return KafkaUser.newBuilder()
                .setId((long) id)
                .setFirstName(FIRST_NAMES[random.nextInt(FIRST_NAMES.length)])
                .setLastName(LAST_NAMES[random.nextInt(LAST_NAMES.length)])
                .setNationality(CountryCode.values()[random.nextInt(CountryCode.values().length)])
                .setBirthDate(Instant.ofEpochSecond(random.nextLong(
                        Instant.parse("1924-01-01T00:00:00Z").getEpochSecond(),
                        Instant.now().getEpochSecond())))
                .build();
    }
}
