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
package io.github.loicgreffier.producer.headers;

import static io.github.loicgreffier.producer.headers.constant.Topic.STRING_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.loicgreffier.producer.headers.app.ProducerRunner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaProducerHeadersApplicationTest {
    @Spy
    private MockProducer<String, String> mockProducer =
            new MockProducer<>(false, new StringSerializer(), new StringSerializer());

    @InjectMocks
    private ProducerRunner producerRunner;

    @Test
    void shouldSendSuccessfully() throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> message = new ProducerRecord<>(STRING_TOPIC, "1", "Message 1");
        Future<RecordMetadata> recordMetadata = producerRunner.send(message);
        mockProducer.completeNext();

        assertTrue(recordMetadata.get().hasOffset());
        assertEquals(0, recordMetadata.get().offset());
        assertEquals(0, recordMetadata.get().partition());
        assertEquals(1, mockProducer.history().size());
        assertEquals(message, mockProducer.history().getFirst());
    }

    @Test
    void shouldSendWithFailure() {
        ProducerRecord<String, String> message = new ProducerRecord<>(STRING_TOPIC, "1", "Message 2");
        Future<RecordMetadata> recordMetadata = producerRunner.send(message);
        RuntimeException exception = new RuntimeException("Error sending message");
        mockProducer.errorNext(exception);

        ExecutionException executionException = assertThrows(ExecutionException.class, recordMetadata::get);
        assertEquals(executionException.getCause(), exception);
        assertEquals(1, mockProducer.history().size());
        assertEquals(message, mockProducer.history().getFirst());
    }
}
