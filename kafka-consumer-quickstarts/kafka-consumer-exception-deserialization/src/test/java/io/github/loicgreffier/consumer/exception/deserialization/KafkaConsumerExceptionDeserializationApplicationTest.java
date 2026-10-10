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
package io.github.loicgreffier.consumer.exception.deserialization;

import static io.github.loicgreffier.consumer.exception.deserialization.constant.Topic.ORDER_TOPIC;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.consumer.exception.deserialization.app.ConsumerRunner;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerExceptionDeserializationApplicationTest {
    @Spy
    private MockConsumer<String, Order> mockConsumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());

    @InjectMocks
    private ConsumerRunner consumerRunner;

    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition(ORDER_TOPIC, 0);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));
    }

    @Test
    void shouldConsumeSuccessfully() {
        ConsumerRecord<String, Order> message = new ConsumerRecord<>(
                ORDER_TOPIC,
                0,
                0,
                "1",
                Order.newBuilder()
                        .setId(1L)
                        .setCustomerId(3L)
                        .setItems(List.of("Laptop", "Mouse"))
                        .setAmount(1249.90)
                        .build());

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());
        verify(mockConsumer).commitSync();
    }

    @Test
    void shouldSkipRecordOnDeserializationException() {
        ConsumerRecord<String, Order> message = new ConsumerRecord<>(
                ORDER_TOPIC,
                0,
                0,
                "1",
                Order.newBuilder()
                        .setId(1L)
                        .setCustomerId(3L)
                        .setItems(List.of("Laptop", "Mouse"))
                        .setAmount(1249.90)
                        .build());

        ConsumerRecord<String, Order> message2 = new ConsumerRecord<>(
                ORDER_TOPIC,
                0,
                2,
                "2",
                Order.newBuilder()
                        .setId(2L)
                        .setCustomerId(3L)
                        .setItems(List.of("Laptop", "Mouse"))
                        .setAmount(1249.90)
                        .build());

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));

        mockConsumer.schedulePollTask(() -> {
            throw new RecordDeserializationException(
                    RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
                    topicPartition,
                    1,
                    0,
                    null,
                    null,
                    null,
                    null,
                    "Error deserializing",
                    new Exception());
        });

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message2));

        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());
        verify(mockConsumer, times(5)).poll(any());
        verify(mockConsumer, times(2)).commitSync();
        verify(mockConsumer).seek(topicPartition, 2);
    }
}
