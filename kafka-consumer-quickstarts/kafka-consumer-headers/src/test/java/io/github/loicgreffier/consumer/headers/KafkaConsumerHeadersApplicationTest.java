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
package io.github.loicgreffier.consumer.headers;

import static io.github.loicgreffier.consumer.headers.constant.Topic.STRING_TOPIC;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.loicgreffier.consumer.headers.app.ConsumerRunner;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerHeadersApplicationTest {
    @Spy
    private MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

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
        String kafkaUser = buildKafkaUser();
        ConsumerRecord<String, String> message = new ConsumerRecord<>(
                STRING_TOPIC, 0, 0, UUID.nameUUIDFromBytes(kafkaUser.getBytes()).toString(), kafkaUser);
        message.headers().add("message", kafkaUser.getBytes());

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());

        verify(mockConsumer).commitSync();
    }

    @Test
    void shouldFailOnPoisonPill() {
        String kafkaUser = buildKafkaUser();
        String key = UUID.nameUUIDFromBytes(kafkaUser.getBytes()).toString();
        ConsumerRecord<String, String> message1 = new ConsumerRecord<>(STRING_TOPIC, 0, 0, key, kafkaUser);
        ConsumerRecord<String, String> message2 = new ConsumerRecord<>(STRING_TOPIC, 0, 2, key, kafkaUser);

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message1));
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

        assertThrows(RecordDeserializationException.class, () -> consumerRunner.run());

        assertTrue(mockConsumer.closed());
        verify(mockConsumer, times(3)).poll(any());
        verify(mockConsumer).commitSync();
    }

    private String buildKafkaUser() {
        return String.format("%s %s", "Homer", "Simpson");
    }
}
