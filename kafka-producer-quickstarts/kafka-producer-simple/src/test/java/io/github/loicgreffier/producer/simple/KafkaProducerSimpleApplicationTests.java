package io.github.loicgreffier.producer.simple;

import static io.github.loicgreffier.producer.simple.constants.Topic.STRING_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.loicgreffier.producer.simple.app.ProducerRunner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
class KafkaProducerSimpleApplicationTests {
    @Spy
    private MockProducer<String, String> mockProducer =
        new MockProducer<>(false, new StringSerializer(), new StringSerializer());

    @InjectMocks
    private ProducerRunner producerRunner;

    @Test
    void shouldSendSuccessfully() throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> message =
            new ProducerRecord<>(STRING_TOPIC, "1", "Message 1");
        Future<RecordMetadata> record = producerRunner.send(message);
        mockProducer.completeNext();

        assertThat(record.get().hasOffset()).isTrue();
        assertThat(record.get().offset()).isZero();
        assertThat(record.get().partition()).isZero();
        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0)).isEqualTo(message);
    }

    @Test
    void shouldSendWithFailure() {
        ProducerRecord<String, String> message =
            new ProducerRecord<>(STRING_TOPIC, "1", "Message 1");
        Future<RecordMetadata> record = producerRunner.send(message);
        RuntimeException exception = new RuntimeException("Error sending message");
        mockProducer.errorNext(exception);

        ExecutionException executionException = assertThrows(ExecutionException.class, record::get);
        assertEquals(executionException.getCause(), exception);
        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0)).isEqualTo(message);
    }
}
