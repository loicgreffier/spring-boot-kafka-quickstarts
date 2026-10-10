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
package io.github.loicgreffier.producer.transaction;

import static io.github.loicgreffier.producer.transaction.constant.Topic.ORDER_JSON_TOPIC;
import static io.github.loicgreffier.producer.transaction.constant.Topic.PAYMENT_JSON_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.loicgreffier.producer.transaction.app.ProducerRunner;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class KafkaProducerTransactionApplicationTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerTransactionApplicationTest.class);

    @Spy
    private MockProducer<String, String> mockProducer =
            new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());

    @InjectMocks
    private ProducerRunner producerRunner;

    @Test
    void shouldCommitTransaction() throws InterruptedException {
        Thread producerThread = new Thread(() -> {
            try {
                producerRunner.run();
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });

        producerThread.start();

        waitForProducer(false);

        ProducerRecord<String, String> orderSentRecord = mockProducer.history().getFirst();

        assertEquals(ORDER_JSON_TOPIC, orderSentRecord.topic());
        assertEquals("1", orderSentRecord.key());
        assertTrue(orderSentRecord.value().startsWith("{\"id\":1,\"customerId\":"));

        ProducerRecord<String, String> paymentSentRecord =
                mockProducer.history().getLast();

        assertEquals(PAYMENT_JSON_TOPIC, paymentSentRecord.topic());
        assertEquals("1", paymentSentRecord.key());
        assertTrue(paymentSentRecord.value().startsWith("{\"id\":1,\"orderId\":1,\"amount\":"));

        assertTrue(mockProducer.transactionInitialized());
        assertTrue(mockProducer.transactionCommitted());
    }

    @Test
    void shouldAbortTransaction() throws InterruptedException {
        Thread producerThread = new Thread(() -> {
            try {
                producerRunner.run();
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });

        producerThread.start();

        waitForProducer(true);

        assertTrue(mockProducer.history().isEmpty());
        assertTrue(mockProducer.transactionInitialized());
        assertTrue(mockProducer.transactionAborted());
    }

    private void waitForProducer(boolean aborted) throws InterruptedException {
        while (aborted ? !mockProducer.transactionAborted() : !mockProducer.transactionCommitted()) {
            log.info("Waiting for producer to produce messages...");
            TimeUnit.MILLISECONDS.sleep(100); // NOSONAR
        }

        producerRunner.setStopped(true);
    }
}
