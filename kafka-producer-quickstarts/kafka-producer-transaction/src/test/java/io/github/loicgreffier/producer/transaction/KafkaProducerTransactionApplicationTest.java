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
package io.github.loicgreffier.producer.transaction;

import static io.github.loicgreffier.producer.transaction.constant.Topic.FIRST_STRING_TOPIC;
import static io.github.loicgreffier.producer.transaction.constant.Topic.SECOND_STRING_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.loicgreffier.producer.transaction.app.ProducerRunner;
import java.util.Arrays;
import java.util.UUID;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaProducerTransactionApplicationTest {
    @Spy
    private MockProducer<String, String> mockProducer =
            new MockProducer<>(false, new StringSerializer(), new StringSerializer());

    @InjectMocks
    private ProducerRunner producerRunner;

    @Test
    void shouldAbortTransaction() {
        mockProducer.initTransactions();

        String kafkaUser = buildKafkaUser("Abraham");
        String key = UUID.nameUUIDFromBytes(kafkaUser.getBytes()).toString();
        String firstName = kafkaUser.split(" ")[0];
        String lastName = kafkaUser.split(" ")[1];

        ProducerRecord<String, String> firstMessage = new ProducerRecord<>(FIRST_STRING_TOPIC, key, firstName);
        ProducerRecord<String, String> secondMessage = new ProducerRecord<>(SECOND_STRING_TOPIC, key, lastName);
        producerRunner.sendInTransaction(Arrays.asList(firstMessage, secondMessage));

        assertTrue(mockProducer.history().isEmpty());
        assertTrue(mockProducer.transactionInitialized());
        assertTrue(mockProducer.transactionAborted());
        assertFalse(mockProducer.transactionCommitted());
        assertFalse(mockProducer.transactionInFlight());
    }

    @Test
    void shouldCommitTransaction() {
        mockProducer.initTransactions();

        String kafkaUser = buildKafkaUser("Homer");
        String key = UUID.nameUUIDFromBytes(kafkaUser.getBytes()).toString();
        String firstName = kafkaUser.split(" ")[0];
        String lastName = kafkaUser.split(" ")[1];

        ProducerRecord<String, String> firstMessage = new ProducerRecord<>(FIRST_STRING_TOPIC, key, firstName);
        ProducerRecord<String, String> secondMessage = new ProducerRecord<>(SECOND_STRING_TOPIC, key, lastName);
        producerRunner.sendInTransaction(Arrays.asList(firstMessage, secondMessage));

        assertEquals(2, mockProducer.history().size());
        assertEquals(FIRST_STRING_TOPIC, mockProducer.history().get(0).topic());
        assertEquals("Homer", mockProducer.history().get(0).value());
        assertEquals(SECOND_STRING_TOPIC, mockProducer.history().get(1).topic());
        assertEquals("Simpson", mockProducer.history().get(1).value());
        assertTrue(mockProducer.transactionInitialized());
        assertTrue(mockProducer.transactionCommitted());
        assertFalse(mockProducer.transactionAborted());
        assertFalse(mockProducer.transactionInFlight());
    }

    /**
     * Builds a Kafka user as a string record.
     *
     * @return The string record.
     */
    private String buildKafkaUser(String firstName) {
        return String.format("%s %s", firstName, "Simpson");
    }
}
