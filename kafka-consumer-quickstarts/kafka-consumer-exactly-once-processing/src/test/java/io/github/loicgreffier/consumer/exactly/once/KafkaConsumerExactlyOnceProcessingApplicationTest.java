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
package io.github.loicgreffier.consumer.exactly.once;

import static io.github.loicgreffier.consumer.exactly.once.constant.Topic.EXACTLY_ONCE_PROCESSING_TOPIC;
import static io.github.loicgreffier.consumer.exactly.once.constant.Topic.ORDER_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.consumer.exactly.once.app.ConsumerRunner;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerExactlyOnceProcessingApplicationTest {
    private final Serializer<Order> serializer = (topic, order) -> {
        KafkaAvroSerializer inner = new KafkaAvroSerializer();
        inner.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        return inner.serialize(topic, order);
    };

    @Spy
    private MockProducer<String, Order> mockProducer =
            new MockProducer<>(true, null, new StringSerializer(), serializer);

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
    void shouldCommitTransaction() {
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

        ProducerRecord<String, Order> sentRecord = mockProducer.history().getFirst();

        assertEquals(EXACTLY_ONCE_PROCESSING_TOPIC, sentRecord.topic());
        assertEquals("1", sentRecord.key());
        assertNotNull(sentRecord.value().getId());
        assertEquals(1499.88, sentRecord.value().getAmount());
        assertTrue(mockProducer.transactionInitialized());
        assertTrue(mockProducer.transactionCommitted());

        assertTrue(mockConsumer.closed());
        verify(mockProducer)
                .sendOffsetsToTransaction(
                        eq(Map.of(new TopicPartition(ORDER_TOPIC, 0), new OffsetAndMetadata(1L))),
                        argThat(argument -> argument.groupId().equals("dummy.group.id")));
    }

    @Test
    void shouldAbortTransaction() {
        ConsumerRecord<String, Order> message = new ConsumerRecord<>(
                ORDER_TOPIC,
                0,
                0,
                "1",
                Order.newBuilder()
                        .setId(1L)
                        .setCustomerId(3L)
                        .setItems(List.of("Laptop", "Mouse"))
                        // Null amount to trigger an exception
                        .build());

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));

        consumerRunner.run();

        assertTrue(mockProducer.history().isEmpty());
        assertTrue(mockProducer.transactionInitialized());
        assertTrue(mockProducer.transactionAborted());

        assertTrue(mockConsumer.closed());
        verify(mockProducer, never()).sendOffsetsToTransaction(any(), any());
    }
}
