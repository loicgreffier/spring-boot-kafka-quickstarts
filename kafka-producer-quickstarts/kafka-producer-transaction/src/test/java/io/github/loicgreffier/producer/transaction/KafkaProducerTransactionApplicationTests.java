package io.github.loicgreffier.producer.transaction;

import static io.github.loicgreffier.producer.transaction.constants.Topic.FIRST_STRING_TOPIC;
import static io.github.loicgreffier.producer.transaction.constants.Topic.SECOND_STRING_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

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
    private MockProducer<String, String> mockProducer =
        new MockProducer<>(false, new StringSerializer(), new StringSerializer());

    @InjectMocks
    private ProducerRunner producerRunner;

    @Test
    void shouldAbortTransaction() {
        mockProducer.initTransactions();

        ProducerRecord<String, String> firstMessage =
            new ProducerRecord<>(FIRST_STRING_TOPIC, "3", "Message 1");
        ProducerRecord<String, String> secondMessage =
            new ProducerRecord<>(SECOND_STRING_TOPIC, "3", "Message 1");
        producerRunner.sendInTransaction(Arrays.asList(firstMessage, secondMessage));

        assertThat(mockProducer.history()).isEmpty();
        assertThat(mockProducer.transactionInitialized()).isTrue();
        assertThat(mockProducer.transactionAborted()).isTrue();
        assertThat(mockProducer.transactionCommitted()).isFalse();
        assertThat(mockProducer.transactionInFlight()).isFalse();
    }

    @Test
    void shouldCommitTransaction() {
        mockProducer.initTransactions();

        ProducerRecord<String, String> firstMessage =
            new ProducerRecord<>(FIRST_STRING_TOPIC, "1", "Message 1");
        ProducerRecord<String, String> secondMessage =
            new ProducerRecord<>(SECOND_STRING_TOPIC, "1", "Message 1");
        producerRunner.sendInTransaction(Arrays.asList(firstMessage, secondMessage));

        assertThat(mockProducer.history()).hasSize(2);
        assertThat(mockProducer.history().get(0).topic()).isEqualTo(FIRST_STRING_TOPIC);
        assertThat(mockProducer.history().get(0).value()).isEqualTo("Message 1");
        assertThat(mockProducer.history().get(1).topic()).isEqualTo(SECOND_STRING_TOPIC);
        assertThat(mockProducer.history().get(1).value()).isEqualTo("Message 1");
        assertThat(mockProducer.transactionInitialized()).isTrue();
        assertThat(mockProducer.transactionCommitted()).isTrue();
        assertThat(mockProducer.transactionAborted()).isFalse();
        assertThat(mockProducer.transactionInFlight()).isFalse();
    }
}
