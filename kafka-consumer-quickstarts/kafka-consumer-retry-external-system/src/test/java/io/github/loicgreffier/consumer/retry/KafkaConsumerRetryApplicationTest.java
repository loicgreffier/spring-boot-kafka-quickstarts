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

package io.github.loicgreffier.consumer.retry;

import static io.github.loicgreffier.consumer.retry.constant.Topic.STRING_TOPIC;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.loicgreffier.consumer.retry.app.ConsumerRunner;
import io.github.loicgreffier.consumer.retry.property.ConsumerProperties;
import io.github.loicgreffier.consumer.retry.service.ExternalService;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerRetryApplicationTest {
    @Spy
    private MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    @Mock
    private ExternalService externalService;

    @Mock
    private ConsumerProperties properties;

    @InjectMocks
    private ConsumerRunner consumerRunner;

    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition(STRING_TOPIC, 0);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));
    }

    @Test
    void shouldConsumeSuccessfully() {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(STRING_TOPIC, 0, 0, "1", "Message 1");

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());

        verify(mockConsumer).commitSync();
    }

    @Test
    void shouldRewindOffsetOnExternalSystemError() throws Exception {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(STRING_TOPIC, 0, 0, "1", "Message 1");
        ConsumerRecord<String, String> message2 = new ConsumerRecord<>(STRING_TOPIC, 0, 1, "2", "Message 2");
        ConsumerRecord<String, String> message3 = new ConsumerRecord<>(STRING_TOPIC, 0, 2, "3", "Message 3");

        // First poll to rewind, second poll to resume
        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> {
                mockConsumer.addRecord(message);
                mockConsumer.addRecord(message2);
                mockConsumer.addRecord(message3);
            });
        }

        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        // Throw exception for message 2, otherwise do nothing
        // Should read from message 2 on second poll loop
        doNothing().when(externalService).call(argThat(arg -> !arg.equals(message2)));
        doThrow(new Exception("Call to external system failed"))
            .when(externalService).call(message2);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());
        verify(mockConsumer).pause(Collections.singleton(topicPartition));
        verify(mockConsumer).seek(topicPartition, new OffsetAndMetadata(1));
        verify(mockConsumer).resume(Collections.singleton(topicPartition));
    }

    @Test
    void shouldRewindToEarliestOnExternalSystemError() throws Exception {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(STRING_TOPIC, 0, 0, "1", "Message 1");

        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        }
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        doThrow(new Exception("Call to external system failed")).when(externalService)
            .call(message);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());
        verify(mockConsumer).pause(Collections.singleton(topicPartition));
        verify(mockConsumer).seekToBeginning(Collections.singleton(topicPartition));
        verify(mockConsumer).resume(Collections.singleton(topicPartition));
    }

    @Test
    void shouldRewindToLatestOnExternalSystemError() throws Exception {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(STRING_TOPIC, 0, 0, "1", "Message 2");

        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        }
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        when(properties.getProperties()).thenReturn(
            Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString()));
        doThrow(new Exception("Call to external system failed")).when(externalService)
            .call(message);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());
        verify(mockConsumer).pause(Collections.singleton(topicPartition));
        verify(mockConsumer).seekToEnd(Collections.singleton(topicPartition));
        verify(mockConsumer).resume(Collections.singleton(topicPartition));
    }
}
