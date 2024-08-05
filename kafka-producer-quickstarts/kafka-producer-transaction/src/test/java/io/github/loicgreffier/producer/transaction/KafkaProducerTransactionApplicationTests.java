package io.github.loicgreffier.producer.transaction;

import static io.github.loicgreffier.producer.transaction.constant.Topic.FIRST_STRING_TOPIC;
import static io.github.loicgreffier.producer.transaction.constant.Topic.SECOND_STRING_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.loicgreffier.producer.transaction.app.ProducerRunner;
import java.util.Arrays;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * This class contains unit tests for the Kafka producer application.
 */
@ExtendWith(MockitoExtension.class)
class KafkaProducerTransactionApplicationTests {
    @Spy
    private MockProducer<String, String> mockProducer = new MockProducer<>(
        false,
        new StringSerializer(),
        new StringSerializer()
    );

    @InjectMocks
    private ProducerRunner producerRunner;

    @Test
    void shouldAbortTransaction() {
        mockProducer.initTransactions();

        ProducerRecord<String, String> firstMessage = new ProducerRecord<>(FIRST_STRING_TOPIC, "3", "John Doe");
        ProducerRecord<String, String> secondMessage = new ProducerRecord<>(SECOND_STRING_TOPIC, "4", "Jane Smith");
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

        ProducerRecord<String, String> firstMessage = new ProducerRecord<>(FIRST_STRING_TOPIC, "1", "John Doe");
        ProducerRecord<String, String> secondMessage = new ProducerRecord<>(SECOND_STRING_TOPIC, "2", "Jane Smith");
        producerRunner.sendInTransaction(Arrays.asList(firstMessage, secondMessage));

        assertEquals(2, mockProducer.history().size());
        assertEquals(FIRST_STRING_TOPIC, mockProducer.history().get(0).topic());
        assertEquals("John Doe", mockProducer.history().get(0).value());
        assertEquals(SECOND_STRING_TOPIC, mockProducer.history().get(1).topic());
        assertEquals("Jane Smith", mockProducer.history().get(1).value());
        assertTrue(mockProducer.transactionInitialized());
        assertTrue(mockProducer.transactionCommitted());
        assertFalse(mockProducer.transactionAborted());
        assertFalse(mockProducer.transactionInFlight());
    }
}
