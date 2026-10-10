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
package io.github.loicgreffier.streams.producer.order.app;

import static io.github.loicgreffier.streams.producer.order.constant.Item.ITEMS;
import static io.github.loicgreffier.streams.producer.order.constant.Topic.ORDER_TOPIC;
import static io.github.loicgreffier.streams.producer.order.constant.Topic.ORDER_TOPIC_TWO;
import static io.github.loicgreffier.streams.producer.order.constant.Topic.PAYMENT_TOPIC;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.Payment;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/** This class represents a Kafka producer runner that sends records to specific topics. */
@Component
public class ProducerRunner {
    private static final Logger log = LoggerFactory.getLogger(ProducerRunner.class);
    private final Random random = new Random();
    private final Producer<String, SpecificRecord> producer;

    /**
     * Constructor.
     *
     * @param producer The Kafka producer
     */
    public ProducerRunner(Producer<String, SpecificRecord> producer) {
        this.producer = producer;
    }

    /**
     * Asynchronously starts the Kafka producer when the application is ready.
     *
     * <p>The {@code @Async} annotation is used to run the producer in a separate thread, preventing it from blocking
     * the main thread.
     *
     * <p>The Kafka producer sends order records to the {@code ORDER_TOPIC} and {@code ORDER_TOPIC_TWO} topics. It also
     * sends a payment record to the {@code PAYMENT_TOPIC} topic for each order of the {@code ORDER_TOPIC} topic, except
     * one order out of five, which remains unpaid. Orders and payments are keyed by order id.
     *
     * @throws InterruptedException if the thread is interrupted while sleeping
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() throws InterruptedException {
        int i = 0;
        while (true) {
            Order order = buildOrder(i);
            send(new ProducerRecord<>(ORDER_TOPIC, String.valueOf(order.getId()), order));
            send(new ProducerRecord<>(ORDER_TOPIC_TWO, String.valueOf(i), buildOrder(i)));

            if (i % 5 != 0) {
                Payment payment = Payment.newBuilder()
                        .setId((long) i)
                        .setOrderId(order.getId())
                        .setAmount(order.getAmount())
                        .build();
                send(new ProducerRecord<>(PAYMENT_TOPIC, String.valueOf(payment.getOrderId()), payment));
            }

            TimeUnit.SECONDS.sleep(1);

            i++;
        }
    }

    /**
     * Sends a message to the Kafka topic.
     *
     * @param message The message to send.
     */
    public void send(ProducerRecord<String, SpecificRecord> message) {
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
     * Builds an order.
     *
     * @param id The order id.
     * @return The order.
     */
    private Order buildOrder(int id) {
        return Order.newBuilder()
                .setId((long) id)
                .setCustomerId((long) random.nextInt(10))
                .setItems(random.ints(random.nextInt(1, 9), 0, ITEMS.size())
                        .mapToObj(ITEMS::get)
                        .toList())
                .setAmount(random.nextInt(1000, 100000) / 100.0)
                .build();
    }
}
