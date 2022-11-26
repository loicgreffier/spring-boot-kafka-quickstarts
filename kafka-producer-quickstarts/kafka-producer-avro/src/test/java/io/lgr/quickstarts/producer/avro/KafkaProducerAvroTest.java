package io.lgr.quickstarts.producer.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.producer.avro.app.KafkaProducerAvroRunner;
import io.lgr.quickstarts.producer.avro.constants.Topic;
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

import static org.assertj.core.api.Assertions.assertThat;

class KafkaProducerAvroTest {
    private KafkaProducerAvroRunner producerRunner;
    private MockProducer<String, KafkaPerson> mockProducer;
    private Serializer<KafkaPerson> serializer;

    @BeforeEach
    void setUp() {
        serializer = (Serializer) new KafkaAvroSerializer();
        serializer.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
    }

    @Test
    void testSendSuccess() throws ExecutionException, InterruptedException {
        mockProducer = new MockProducer<>(true, new StringSerializer(), serializer);
        producerRunner = new KafkaProducerAvroRunner(mockProducer);

        ProducerRecord<String, KafkaPerson> message = new ProducerRecord<>(Topic.PERSON_TOPIC.toString(), "1", KafkaPerson.newBuilder()
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
    void testSendFailure() {
        mockProducer = new MockProducer<>(false, new StringSerializer(), serializer);
        producerRunner = new KafkaProducerAvroRunner(mockProducer);

        ProducerRecord<String, KafkaPerson> message = new ProducerRecord<>(Topic.PERSON_TOPIC.toString(), "1", KafkaPerson.newBuilder()
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

        try {
            record.get();
        } catch (ExecutionException | InterruptedException ex) {
            assert(ex.getCause()).equals(exception);
        }
    }
}
