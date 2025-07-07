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

import static io.github.loicgreffier.streams.producer.order.constant.Name.ITEMS;
import static io.github.loicgreffier.streams.producer.order.constant.Topic.ORDER_TOPIC;

import io.github.loicgreffier.avro.KafkaOrder;
import io.github.loicgreffier.streams.producer.order.constant.Name;
import java.util.ArrayList;
import java.util.List;
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
    private final Producer<String, KafkaOrder> producer;

    /**
     * Constructor.
     *
     * @param producer The Kafka producer
     */
    public ProducerRunner(Producer<String, KafkaOrder> producer) {
        this.producer = producer;
    }

    /**
     * Asynchronously starts the Kafka producer when the application is ready.
     *
     * <p>The {@code @Async} annotation is used to run the producer in a separate thread, preventing it from blocking
     * the main thread.
     *
     * <p>The Kafka producer sends order records to the {@code ORDER_TOPIC}.
     *
     * @throws InterruptedException if the thread is interrupted while sleeping
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() throws InterruptedException {
        long i = 0;
        while (true) {
            ProducerRecord<String, KafkaOrder> message =
                    new ProducerRecord<>(ORDER_TOPIC, String.valueOf(i), buildKafkaOrder(i));

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

            TimeUnit.SECONDS.sleep(1);

            i++;
        }
    }

    /**
     * Builds a Kafka order.
     *
     * @param id The order id.
     * @return The Kafka order.
     */
    private KafkaOrder buildKafkaOrder(long id) {
        List<String> selectedItems = new ArrayList<>();
        int itemCount = random.nextInt(10) + 1;
        double totalAmount = 0.0;

        for (int i = 0; i < itemCount; i++) {
            Name.Item item = ITEMS[random.nextInt(ITEMS.length)];
            selectedItems.add(item.getName());
            totalAmount += item.getPrice();
        }

        return KafkaOrder.newBuilder()
                .setId(id)
                .setItems(selectedItems)
                .setTotalAmount(totalAmount)
                .setCustomerId(id)
                .build();
    }
}
