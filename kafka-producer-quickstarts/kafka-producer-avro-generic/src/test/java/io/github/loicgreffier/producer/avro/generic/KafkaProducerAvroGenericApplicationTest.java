package io.github.loicgreffier.producer.avro.generic;

import static io.github.loicgreffier.producer.avro.generic.constant.Topic.PERSON_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.github.loicgreffier.producer.avro.generic.app.ProducerRunner;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.ClassPathResource;

/**
 * This class contains unit tests for the Kafka producer application.
 */
@ExtendWith(MockitoExtension.class)
class KafkaProducerAvroGenericApplicationTest {
    private final Serializer<GenericRecord> serializer = (topic, genericRecord) -> {
        KafkaAvroSerializer inner = new KafkaAvroSerializer();
        inner.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
        return inner.serialize(topic, genericRecord);
    };

    @Spy
    private MockProducer<String, GenericRecord> mockProducer = new MockProducer<>(
        false,
        new StringSerializer(),
        serializer
    );

    @InjectMocks
    private ProducerRunner producerRunner;

    @Test
    void shouldSendSuccessfully() throws ExecutionException, InterruptedException, IOException {
        File schemaFile = new ClassPathResource("person.avsc").getFile();
        Schema schema = new Schema.Parser().parse(schemaFile);

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", 1L);
        genericRecord.put("firstName", "John");
        genericRecord.put("lastName", "Doe");
        genericRecord.put("birthDate", System.currentTimeMillis());

        ProducerRecord<String, GenericRecord> message = new ProducerRecord<>(PERSON_TOPIC, "1", genericRecord);

        Future<RecordMetadata> record = producerRunner.send(message);
        mockProducer.completeNext();

        assertTrue(record.get().hasOffset());
        assertEquals(0, record.get().offset());
        assertEquals(0, record.get().partition());
        assertEquals(1, mockProducer.history().size());
        assertEquals(message, mockProducer.history().get(0));
    }

    @Test
    void shouldSendWithFailure() throws IOException {
        File schemaFile = new ClassPathResource("person.avsc").getFile();
        Schema schema = new Schema.Parser().parse(schemaFile);

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", 1L);
        genericRecord.put("firstName", "John");
        genericRecord.put("lastName", "Doe");
        genericRecord.put("birthDate", System.currentTimeMillis());

        ProducerRecord<String, GenericRecord> message = new ProducerRecord<>(PERSON_TOPIC, "1", genericRecord);

        Future<RecordMetadata> record = producerRunner.send(message);
        RuntimeException exception = new RuntimeException("Error sending message");
        mockProducer.errorNext(exception);

        ExecutionException executionException = assertThrows(ExecutionException.class, record::get);
        assertEquals(executionException.getCause(), exception);
        assertEquals(1, mockProducer.history().size());
        assertEquals(message, mockProducer.history().get(0));
    }

    @Test
    void shouldNotSerializeGenericRecordWhenWrongFieldType() throws IOException {
        File schemaFile = new ClassPathResource("person.avsc").getFile();
        Schema schema = new Schema.Parser().parse(schemaFile);

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", "aStringThatShouldBeALong");
        genericRecord.put("firstName", "John");
        genericRecord.put("lastName", "Doe");
        genericRecord.put("birthDate", System.currentTimeMillis());

        ProducerRecord<String, GenericRecord> message = new ProducerRecord<>(PERSON_TOPIC, "1", genericRecord);

        SerializationException serializationException =
            assertThrows(SerializationException.class, () -> producerRunner.send(message));

        assertEquals("Error serializing Avro message", serializationException.getMessage());
        assertEquals("Not in union [\"null\",\"long\"]: aStringThatShouldBeALong (field=id)",
            serializationException.getCause().getMessage());
    }
}
