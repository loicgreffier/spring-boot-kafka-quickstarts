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
package io.github.loicgreffier.streams.producer.customer.app;

import static io.github.loicgreffier.streams.producer.customer.constant.Name.FIRST_NAMES;
import static io.github.loicgreffier.streams.producer.customer.constant.Name.LAST_NAMES;
import static io.github.loicgreffier.streams.producer.customer.constant.Topic.CUSTOMER_TOPIC;

import io.github.loicgreffier.avro.Customer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/** This class represents a Kafka producer runner that sends records to a specific topic. */
@Component
public class ProducerRunner {
    private static final Logger log = LoggerFactory.getLogger(ProducerRunner.class);
    private static final int CUSTOMER_COUNT = 10;
    private final Producer<String, Customer> producer;

    /**
     * Constructor.
     *
     * @param producer The Kafka producer
     */
    public ProducerRunner(Producer<String, Customer> producer) {
        this.producer = producer;
    }

    /**
     * Asynchronously starts the Kafka producer when the application is ready.
     *
     * <p>The {@code @Async} annotation is used to run the producer in a separate thread, preventing it from blocking
     * the main thread.
     *
     * <p>The Kafka producer sends the customer referential to the {@code CUSTOMER_TOPIC}. The customer ids match the
     * customer ids of the orders.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        for (int i = 0; i < CUSTOMER_COUNT; i++) {
            Customer customer = buildCustomer(i);
            ProducerRecord<String, Customer> message =
                    new ProducerRecord<>(CUSTOMER_TOPIC, String.valueOf(customer.getId()), customer);

            send(message);
        }
    }

    /**
     * Sends a message to the Kafka topic.
     *
     * @param message The message to send.
     */
    public void send(ProducerRecord<String, Customer> message) {
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
     * Builds a customer of the referential.
     *
     * @param id The customer id.
     * @return The customer.
     */
    private Customer buildCustomer(int id) {
        String firstName = FIRST_NAMES.get(id % FIRST_NAMES.size());
        String lastName = LAST_NAMES.get(id % LAST_NAMES.size());

        return Customer.newBuilder()
                .setId((long) id)
                .setFirstName(firstName)
                .setLastName(lastName)
                .setEmail("%s.%s@mail.com".formatted(firstName, lastName).toLowerCase())
                .build();
    }
}
