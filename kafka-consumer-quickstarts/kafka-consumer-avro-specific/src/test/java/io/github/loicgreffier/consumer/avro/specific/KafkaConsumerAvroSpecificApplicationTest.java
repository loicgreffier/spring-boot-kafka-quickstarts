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
package io.github.loicgreffier.consumer.avro.specific;

import static io.github.loicgreffier.consumer.avro.specific.constant.Topic.USER_TOPIC;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.consumer.avro.specific.app.ConsumerRunner;
import java.time.Instant;
import java.util.Collections;
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
class KafkaConsumerAvroSpecificApplicationTest {
    @Spy
    private MockConsumer<String, KafkaUser> mockConsumer = new MockConsumer<>(AutoOffsetResetStrategy.EARLIEST.name());

    @InjectMocks
    private ConsumerRunner consumerRunner;

    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition(USER_TOPIC, 0);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));
    }

    @Test
    void shouldConsumeSuccessfully() {
        ConsumerRecord<String, KafkaUser> message = new ConsumerRecord<>(
                USER_TOPIC,
                0,
                0,
                "1",
                KafkaUser.newBuilder()
                        .setId(1L)
                        .setFirstName("Homer")
                        .setLastName("Simpson")
                        .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
                        .build());

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());
        verify(mockConsumer).commitSync();
    }

    @Test
    void shouldFailOnPoisonPill() {
        ConsumerRecord<String, KafkaUser> message = new ConsumerRecord<>(
                USER_TOPIC,
                0,
                0,
                "1",
                KafkaUser.newBuilder()
                        .setId(1L)
                        .setFirstName("Homer")
                        .setLastName("Simpson")
                        .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
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
        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));

        assertThrows(RecordDeserializationException.class, () -> consumerRunner.run());

        assertTrue(mockConsumer.closed());
        verify(mockConsumer, times(3)).poll(any());
        verify(mockConsumer).commitSync();
    }
}
