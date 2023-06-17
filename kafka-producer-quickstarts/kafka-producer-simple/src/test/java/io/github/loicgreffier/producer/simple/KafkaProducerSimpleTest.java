package io.github.loicgreffier.producer.simple;

import io.github.loicgreffier.producer.simple.app.KafkaProducerSimpleRunner;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.github.loicgreffier.producer.simple.constants.Topic.STRING_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaProducerSimpleTest {
    private KafkaProducerSimpleRunner producerRunner;
    private MockProducer<String, String> mockProducer;

    @Test
    void shouldSendSuccessfully() throws ExecutionException, InterruptedException {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        producerRunner = new KafkaProducerSimpleRunner(mockProducer);

        ProducerRecord<String, String> message = new ProducerRecord<>(STRING_TOPIC, "1", "Message 1");
        Future<RecordMetadata> record = producerRunner.send(message);

        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0)).isEqualTo(message);
        assertThat(record.get().hasOffset()).isTrue();
        assertThat(record.get().offset()).isZero();
        assertThat(record.get().partition()).isZero();
    }

    @Test
    void shouldSendWithFailure() {
        mockProducer = new MockProducer<>(false, new StringSerializer(), new StringSerializer());
        producerRunner = new KafkaProducerSimpleRunner(mockProducer);

        ProducerRecord<String, String> message = new ProducerRecord<>(STRING_TOPIC, "1", "Message 1");
        Future<RecordMetadata> record = producerRunner.send(message);
        RuntimeException exception = new RuntimeException("Error sending message");
        mockProducer.errorNext(exception);

        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0)).isEqualTo(message);

        ExecutionException executionException = assertThrows(ExecutionException.class, record::get);

        assertEquals(executionException.getCause(), exception);
    }
}
