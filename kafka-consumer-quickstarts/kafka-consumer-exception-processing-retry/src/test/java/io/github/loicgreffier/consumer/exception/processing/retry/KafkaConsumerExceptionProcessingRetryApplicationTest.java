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
package io.github.loicgreffier.consumer.exception.processing.retry;

import static io.github.loicgreffier.consumer.exception.processing.retry.constant.Topic.ORDER_JSON_TOPIC;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import io.github.loicgreffier.consumer.exception.processing.retry.app.ConsumerRunner;
import io.github.loicgreffier.consumer.exception.processing.retry.property.ConsumerProperties;
import io.github.loicgreffier.consumer.exception.processing.retry.service.ExternalService;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerExceptionProcessingRetryApplicationTest {
    @Spy
    private MockConsumer<String, String> mockConsumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());

    @Mock
    private ExternalService externalService;

    private ConsumerProperties properties;
    private ConsumerRunner consumerRunner;
    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        properties = new ConsumerProperties();
        consumerRunner = new ConsumerRunner(mockConsumer, properties, externalService);
        topicPartition = new TopicPartition(ORDER_JSON_TOPIC, 0);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));
    }

    @Test
    void shouldConsumeSuccessfully() throws InterruptedException {
        doNothing().when(externalService).call(any());

        ConsumerRecord<String, String> message = new ConsumerRecord<>(
                ORDER_JSON_TOPIC,
                0,
                0,
                "ORD1",
                "{\"id\":\"ORD1\",\"customerId\":\"CUS1\",\"items\":[\"Laptop\",\"Mouse\"],\"amount\":1249.90}");

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());

        verify(mockConsumer).commitSync(Map.of(topicPartition, new OffsetAndMetadata(1L)));
    }

    @Test
    void shouldRewindOffsetOnExternalSystemError() throws InterruptedException {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(
                ORDER_JSON_TOPIC,
                0,
                0,
                "ORD1",
                "{\"id\":\"ORD1\",\"customerId\":\"CUS1\",\"items\":[\"Laptop\",\"Mouse\"],\"amount\":1249.90}");
        ConsumerRecord<String, String> messageTwo = new ConsumerRecord<>(
                ORDER_JSON_TOPIC,
                0,
                1,
                "ORD2",
                "{\"id\":\"ORD2\",\"customerId\":\"CUS2\",\"items\":[\"Keyboard\"],\"amount\":89.99}");
        ConsumerRecord<String, String> messageThree = new ConsumerRecord<>(
                ORDER_JSON_TOPIC,
                0,
                2,
                "ORD3",
                "{\"id\":\"ORD3\",\"customerId\":\"CUS3\",\"items\":[\"Monitor\"],\"amount\":219.00}");

        // First poll to rewind, second poll to resume
        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> {
                mockConsumer.addRecord(message);
                mockConsumer.addRecord(messageTwo);
                mockConsumer.addRecord(messageThree);
            });
        }

        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        // Throw exception for message 2, otherwise do nothing
        // Should read from message 2 on second poll loop
        doNothing().when(externalService).call(argThat(arg -> !arg.equals(messageTwo)));
        doThrow(new IllegalStateException("Call to external system failed"))
                .when(externalService)
                .call(messageTwo);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());
        verify(mockConsumer).pause(Collections.singleton(topicPartition));
        verify(mockConsumer).seek(topicPartition, new OffsetAndMetadata(1));
        verify(mockConsumer).resume(Collections.singleton(topicPartition));
    }

    @Test
    void shouldRewindToEarliestOnExternalSystemError() throws InterruptedException {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(
                ORDER_JSON_TOPIC,
                0,
                0,
                "ORD1",
                "{\"id\":\"ORD1\",\"customerId\":\"CUS1\",\"items\":[\"Laptop\",\"Mouse\"],\"amount\":1249.90}");

        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        }
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        doThrow(new IllegalStateException("Call to external system failed"))
                .when(externalService)
                .call(message);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());
        verify(mockConsumer).pause(Collections.singleton(topicPartition));
        verify(mockConsumer).seekToBeginning(Collections.singleton(topicPartition));
        verify(mockConsumer).resume(Collections.singleton(topicPartition));
    }

    @Test
    void shouldRewindToLatestOnExternalSystemError() throws InterruptedException {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(
                ORDER_JSON_TOPIC,
                0,
                0,
                "ORD1",
                "{\"id\":\"ORD1\",\"customerId\":\"CUS1\",\"items\":[\"Laptop\",\"Mouse\"],\"amount\":1249.90}");

        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        }
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        properties.getProperties().put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetStrategy.LATEST.name());

        doThrow(new IllegalStateException("Call to external system failed"))
                .when(externalService)
                .call(message);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());
        verify(mockConsumer).pause(Collections.singleton(topicPartition));
        verify(mockConsumer).seekToEnd(Collections.singleton(topicPartition));
        verify(mockConsumer).resume(Collections.singleton(topicPartition));
    }
}
