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

import static io.github.loicgreffier.producer.transaction.constant.Item.ITEMS;
import static io.github.loicgreffier.producer.transaction.constant.Topic.ORDER_JSON_TOPIC;
import static io.github.loicgreffier.producer.transaction.constant.Topic.PAYMENT_JSON_TOPIC;

import io.github.loicgreffier.producer.transaction.model.Order;
import io.github.loicgreffier.producer.transaction.model.Payment;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
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
    private final Random random = new Random();
    private final Producer<String, String> producer;
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
     * <p>The Kafka producer sends an order and its payment, serialized as JSON strings, to two topics:
     * {@code ORDER_JSON_TOPIC} and {@code PAYMENT_JSON_TOPIC}, within a single transaction. Either both records are
     * successfully committed as part of the transaction, or both are discarded if the transaction fails.
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
            Order order = buildOrder(i);
            Payment payment = new Payment(i, order.id(), order.amount());

            ProducerRecord<String, String> orderMessage =
                    new ProducerRecord<>(ORDER_JSON_TOPIC, String.valueOf(order.id()), order.toJson());

            ProducerRecord<String, String> paymentMessage =
                    new ProducerRecord<>(PAYMENT_JSON_TOPIC, String.valueOf(payment.orderId()), payment.toJson());

            List<ProducerRecord<String, String>> messages = List.of(orderMessage, paymentMessage);

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

                if (order.id() % 3 == 0) {
                    throw new IllegalStateException("Error during transaction...");
                }

                log.info("Commit transaction");
                producer.commitTransaction();
            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException _) {
                log.info("Closing producer");
                producer.close();
                stopped = true;
            } catch (Exception e) {
                log.error("Abort transaction", e);
                producer.abortTransaction();
            }

            TimeUnit.SECONDS.sleep(1);

            i++;
        }
    }

    /**
     * Set whether the runner is stopped.
     *
     * @param stopped {@code true} to stop the runner; {@code false} otherwise.
     */
    public void setStopped(boolean stopped) {
        this.stopped = stopped;
    }

    /**
     * Builds an order.
     *
     * @param id The order id.
     * @return The order.
     */
    private Order buildOrder(int id) {
        return new Order(
                id,
                random.nextInt(10),
                random.ints(random.nextInt(1, 9), 0, ITEMS.size())
                        .mapToObj(ITEMS::get)
                        .toList(),
                random.nextInt(1000, 100000) / 100.0);
    }
}
