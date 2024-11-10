package io.github.loicgreffier.producer.avro.specific;

import static io.github.loicgreffier.producer.avro.specific.constant.Topic.PERSON_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.producer.avro.specific.app.ProducerRunner;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaProducerAvroSpecificApplicationTest {
    private final Serializer<KafkaPerson> serializer = (topic, kafkaPerson) -> {
        KafkaAvroSerializer inner = new KafkaAvroSerializer();
        inner.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        return inner.serialize(topic, kafkaPerson);
    };

    @Spy
    private MockProducer<String, KafkaPerson> mockProducer = new MockProducer<>(
        false,
        new StringSerializer(),
        serializer
    );

    @InjectMocks
    private ProducerRunner producerRunner;

    @Test
    void shouldSendSuccessfully() throws ExecutionException, InterruptedException {
        ProducerRecord<String, KafkaPerson> message = new ProducerRecord<>(PERSON_TOPIC, "1", KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName("Homer")
            .setLastName("Simpson")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build());

        Future<RecordMetadata> recordMetadata = producerRunner.send(message);
        mockProducer.completeNext();

        assertTrue(recordMetadata.get().hasOffset());
        assertEquals(0, recordMetadata.get().offset());
        assertEquals(0, recordMetadata.get().partition());
        assertEquals(1, mockProducer.history().size());
        assertEquals(message, mockProducer.history().getFirst());
    }

    @Test
    void shouldSendWithFailure() {
        ProducerRecord<String, KafkaPerson> message = new ProducerRecord<>(PERSON_TOPIC, "1", KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName("Homer")
            .setLastName("Simpson")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build());

        Future<RecordMetadata> recordMetadata = producerRunner.send(message);
        RuntimeException exception = new RuntimeException("Error sending message");
        mockProducer.errorNext(exception);

        ExecutionException executionException = assertThrows(ExecutionException.class, recordMetadata::get);
        assertEquals(executionException.getCause(), exception);
        assertEquals(1, mockProducer.history().size());
        assertEquals(message, mockProducer.history().getFirst());
    }
}
