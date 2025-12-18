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

import static io.github.loicgreffier.producer.transaction.constant.Topic.FIRST_STRING_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.loicgreffier.producer.transaction.app.ProducerRunner;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@Slf4j
@ExtendWith(MockitoExtension.class)
class KafkaProducerTransactionApplicationTest {
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

        ProducerRecord<String, String> firstSentRecord = mockProducer.history().getFirst();

        assertEquals(FIRST_STRING_TOPIC, firstSentRecord.topic());
        assertEquals("1", firstSentRecord.key());
        assertEquals("Message 1", firstSentRecord.value());

        ProducerRecord<String, String> secondSentRecord = mockProducer.history().getFirst();

        assertEquals(FIRST_STRING_TOPIC, secondSentRecord.topic());
        assertEquals("1", secondSentRecord.key());
        assertEquals("Message 1", secondSentRecord.value());

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
