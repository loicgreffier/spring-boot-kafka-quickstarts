package io.github.loicgreffier.producer.avro.specific;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.producer.avro.specific.app.KafkaProducerAvroSpecificRunner;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.github.loicgreffier.producer.avro.specific.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaProducerAvroSpecificTest {
    private KafkaProducerAvroSpecificRunner producerRunner;
    private MockProducer<String, KafkaPerson> mockProducer;
    private Serializer<KafkaPerson> serializer;

    @BeforeEach
    void setUp() {
        serializer = (Serializer) new KafkaAvroSerializer();
        serializer.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
    }

    @Test
    void shouldSendSuccessfully() throws ExecutionException, InterruptedException {
        mockProducer = new MockProducer<>(true, new StringSerializer(), serializer);
        producerRunner = new KafkaProducerAvroSpecificRunner(mockProducer);

        ProducerRecord<String, KafkaPerson> message = new ProducerRecord<>(PERSON_TOPIC, "1", KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("First name")
                .setLastName("Last name")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build());

        Future<RecordMetadata> record = producerRunner.send(message);

        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0)).isEqualTo(message);
        assertThat(record.get().hasOffset()).isTrue();
        assertThat(record.get().offset()).isZero();
        assertThat(record.get().partition()).isZero();
    }

    @Test
    void shouldSendWithFailure() {
        mockProducer = new MockProducer<>(false, new StringSerializer(), serializer);
        producerRunner = new KafkaProducerAvroSpecificRunner(mockProducer);

        ProducerRecord<String, KafkaPerson> message = new ProducerRecord<>(PERSON_TOPIC, "1", KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("First name")
                .setLastName("Last name")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build());

        Future<RecordMetadata> record = producerRunner.send(message);
        RuntimeException exception = new RuntimeException("Error sending message");
        mockProducer.errorNext(exception);

        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0)).isEqualTo(message);

        ExecutionException executionException = assertThrows(ExecutionException.class, record::get);

        assertEquals(executionException.getCause(), exception);
    }
}
