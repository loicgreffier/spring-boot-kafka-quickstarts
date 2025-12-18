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
package io.github.loicgreffier.producer.simple;

import static io.github.loicgreffier.producer.simple.constant.Topic.STRING_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.loicgreffier.producer.simple.app.ProducerRunner;
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
class KafkaProducerSimpleApplicationTest {
    @Spy
    private MockProducer<String, String> mockProducer =
            new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());

    @InjectMocks
    private ProducerRunner producerRunner;

    @Test
    void shouldSendAutomaticallyWithSuccess() throws InterruptedException {
        Thread producerThread = new Thread(() -> {
            try {
                producerRunner.run();
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            }
        });

        producerThread.start();

        waitForProducer();

        ProducerRecord<String, String> sentRecord = mockProducer.history().getFirst();

        assertEquals(STRING_TOPIC, sentRecord.topic());
        assertEquals("0", sentRecord.key());
        assertEquals("Message 0", sentRecord.value());
    }

    private void waitForProducer() throws InterruptedException {
        while (mockProducer.history().isEmpty()) {
            log.info("Waiting for producer to produce messages...");
            TimeUnit.MILLISECONDS.sleep(100); // NOSONAR
        }

        producerRunner.setStopped(true);
    }
}
