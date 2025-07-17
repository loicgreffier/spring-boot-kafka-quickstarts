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
import static io.github.loicgreffier.consumer.exactly.once.constant.Topic.USER_TOPIC;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.consumer.exactly.once.app.ConsumerRunner;
import java.time.Instant;
import java.util.Collections;
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
public class KafkaConsumerExactlyOnceProcessingApplicationTest {
    private final Serializer<KafkaUser> serializer = (topic, kafkaUser) -> {
        KafkaAvroSerializer inner = new KafkaAvroSerializer();
        inner.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        return inner.serialize(topic, kafkaUser);
    };

    @Spy
    private MockProducer<String, KafkaUser> mockProducer =
            new MockProducer<>(true, null, new StringSerializer(), serializer);

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
    void shouldCommitTransaction() {
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

        ProducerRecord<String, KafkaUser> sentRecord = mockProducer.history().getFirst();

        assertEquals(EXACTLY_ONCE_PROCESSING_TOPIC, sentRecord.topic());
        assertEquals("1", sentRecord.key());
        assertNotNull(sentRecord.value().getId());
        assertNotNull(sentRecord.value().getFirstName());
        assertNotNull(sentRecord.value().getLastName());
        assertNotNull(sentRecord.value().getBirthDate());
        assertTrue(mockProducer.transactionInitialized());
        assertTrue(mockProducer.transactionCommitted());

        assertTrue(mockConsumer.closed());
        verify(mockProducer)
                .sendOffsetsToTransaction(
                        eq(Map.of(new TopicPartition(USER_TOPIC, 0), new OffsetAndMetadata(1L))),
                        argThat(argument -> argument.groupId().equals("dummy.group.id")));
    }

    @Test
    void shouldAbortTransaction() {
        ConsumerRecord<String, KafkaUser> message = new ConsumerRecord<>(
                USER_TOPIC,
                0,
                0,
                "1",
                KafkaUser.newBuilder()
                        .setId(1L)
                        // Null first name to trigger an exception
                        .setLastName("Simpson")
                        .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
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
