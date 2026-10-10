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
package io.github.loicgreffier.producer.avro.specific;

import static io.github.loicgreffier.producer.avro.specific.constant.Topic.ORDER_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.producer.avro.specific.app.ProducerRunner;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
class KafkaProducerAvroSpecificApplicationTest {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerAvroSpecificApplicationTest.class);

    private final Serializer<Order> serializer = (topic, order) -> {
        KafkaAvroSerializer inner = new KafkaAvroSerializer();
        inner.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        return inner.serialize(topic, order);
    };

    @Spy
    private MockProducer<String, Order> mockProducer =
            new MockProducer<>(true, null, new StringSerializer(), serializer);

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

        ProducerRecord<String, Order> sentRecord = mockProducer.history().getFirst();

        assertEquals(ORDER_TOPIC, sentRecord.topic());
        assertEquals("0", sentRecord.key());
        assertNotNull(sentRecord.value().getId());
        assertNotNull(sentRecord.value().getCustomerId());
        assertFalse(sentRecord.value().getItems().isEmpty());
        assertNotNull(sentRecord.value().getAmount());
    }

    private void waitForProducer() throws InterruptedException {
        while (mockProducer.history().isEmpty()) {
            log.info("Waiting for producer to produce messages...");
            TimeUnit.MILLISECONDS.sleep(100); // NOSONAR
        }

        producerRunner.setStopped(true);
    }
}
