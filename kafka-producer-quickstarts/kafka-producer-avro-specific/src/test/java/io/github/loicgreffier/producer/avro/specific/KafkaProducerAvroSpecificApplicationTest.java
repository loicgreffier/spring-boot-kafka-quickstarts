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

import static io.github.loicgreffier.producer.avro.specific.constant.Topic.USER_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.producer.avro.specific.app.ProducerRunner;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@Slf4j
@ExtendWith(MockitoExtension.class)
class KafkaProducerAvroSpecificApplicationTest {
    private final Serializer<KafkaUser> serializer = (topic, kafkaUser) -> {
        KafkaAvroSerializer inner = new KafkaAvroSerializer();
        inner.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        return inner.serialize(topic, kafkaUser);
    };

    @Spy
    private MockProducer<String, KafkaUser> mockProducer =
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

        ProducerRecord<String, KafkaUser> sentRecord = mockProducer.history().getFirst();

        assertEquals(USER_TOPIC, sentRecord.topic());
        assertEquals("0", sentRecord.key());
        assertNotNull(sentRecord.value().getId());
        assertNotNull(sentRecord.value().getFirstName());
        assertNotNull(sentRecord.value().getLastName());
        assertNotNull(sentRecord.value().getBirthDate());
    }

    private void waitForProducer() throws InterruptedException {
        while (mockProducer.history().isEmpty()) {
            log.info("Waiting for producer to produce messages...");
            TimeUnit.MILLISECONDS.sleep(100); // NOSONAR
        }

        producerRunner.setStopped(true);
    }
}
