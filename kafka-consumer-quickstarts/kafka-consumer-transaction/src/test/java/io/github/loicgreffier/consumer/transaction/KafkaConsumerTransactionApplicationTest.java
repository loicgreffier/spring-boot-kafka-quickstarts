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
package io.github.loicgreffier.consumer.transaction;

import static io.github.loicgreffier.consumer.transaction.constant.Topic.FIRST_STRING_TOPIC;
import static io.github.loicgreffier.consumer.transaction.constant.Topic.SECOND_STRING_TOPIC;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

import io.github.loicgreffier.consumer.transaction.app.ConsumerRunner;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerTransactionApplicationTest {
    @Spy
    private MockConsumer<String, String> mockConsumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());

    @InjectMocks
    private ConsumerRunner consumerRunner;

    @BeforeEach
    void setUp() {
        TopicPartition firstTopicPartition = new TopicPartition(FIRST_STRING_TOPIC, 0);
        TopicPartition secondTopicPartition = new TopicPartition(SECOND_STRING_TOPIC, 0);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(List.of(firstTopicPartition, secondTopicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(firstTopicPartition, 0L, secondTopicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(firstTopicPartition, 0L, secondTopicPartition, 0L));
    }

    @Test
    void shouldConsumeFromBothTopicsSuccessfully() {
        ConsumerRecord<String, String> firstMessage = new ConsumerRecord<>(FIRST_STRING_TOPIC, 0, 0, "1", "Message 1");
        ConsumerRecord<String, String> secondMessage =
                new ConsumerRecord<>(SECOND_STRING_TOPIC, 0, 0, "2", "Message 2");

        mockConsumer.schedulePollTask(() -> {
            mockConsumer.addRecord(firstMessage);
            mockConsumer.addRecord(secondMessage);
        });
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());

        verify(mockConsumer).commitSync();
    }
}
