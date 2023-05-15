package io.github.loicgreffier.producer.avro.generic;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.github.loicgreffier.producer.avro.generic.app.KafkaProducerAvroGenericRunner;
import io.github.loicgreffier.producer.avro.generic.constants.Topic;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaProducerAvroGenericTest {
    private KafkaProducerAvroGenericRunner producerRunner;
    private MockProducer<String, GenericRecord> mockProducer;
    private Serializer<GenericRecord> serializer;

    @BeforeEach
    void setUp() {
        serializer = (Serializer) new KafkaAvroSerializer();
        serializer.configure(Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://"), false);
    }

    @Test
    void shouldSendSuccessfully() throws ExecutionException, InterruptedException, IOException {
        mockProducer = new MockProducer<>(true, new StringSerializer(), serializer);
        producerRunner = new KafkaProducerAvroGenericRunner(mockProducer);

        File schemaFile = new ClassPathResource("person.avsc").getFile();
        Schema schema = new Schema.Parser().parse(schemaFile);

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", 1L);
        genericRecord.put("firstName", "First name");
        genericRecord.put("lastName", "Last name");
        genericRecord.put("birthDate", System.currentTimeMillis());

        ProducerRecord<String, GenericRecord> message = new ProducerRecord<>(Topic.PERSON_TOPIC.toString(), "1", genericRecord);

        Future<RecordMetadata> record = producerRunner.send(message);

        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0)).isEqualTo(message);
        assertThat(record.get().hasOffset()).isTrue();
        assertThat(record.get().offset()).isZero();
        assertThat(record.get().partition()).isZero();
    }

    @Test
    void shouldSendWithFailure() throws IOException {
        mockProducer = new MockProducer<>(false, new StringSerializer(), serializer);
        producerRunner = new KafkaProducerAvroGenericRunner(mockProducer);

        File schemaFile = new ClassPathResource("person.avsc").getFile();
        Schema schema = new Schema.Parser().parse(schemaFile);

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", 1L);
        genericRecord.put("firstName", "First name");
        genericRecord.put("lastName", "Last name");
        genericRecord.put("birthDate", System.currentTimeMillis());

        ProducerRecord<String, GenericRecord> message = new ProducerRecord<>(Topic.PERSON_TOPIC.toString(), "1", genericRecord);

        Future<RecordMetadata> record = producerRunner.send(message);
        RuntimeException exception = new RuntimeException("Error sending message");
        mockProducer.errorNext(exception);

        assertThat(mockProducer.history()).hasSize(1);
        assertThat(mockProducer.history().get(0)).isEqualTo(message);

        ExecutionException executionException = assertThrows(ExecutionException.class, record::get);

        assertEquals(executionException.getCause(), exception);
    }

    @Test
    void shouldNotSerializeGenericRecordWhenWrongFieldType() throws IOException {
        mockProducer = new MockProducer<>(false, new StringSerializer(), serializer);
        producerRunner = new KafkaProducerAvroGenericRunner(mockProducer);

        File schemaFile = new ClassPathResource("person.avsc").getFile();
        Schema schema = new Schema.Parser().parse(schemaFile);

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", "aStringThatShouldBeALong");
        genericRecord.put("firstName", "First name");
        genericRecord.put("lastName", "Last name");
        genericRecord.put("birthDate", System.currentTimeMillis());

        ProducerRecord<String, GenericRecord> message = new ProducerRecord<>(Topic.PERSON_TOPIC.toString(), "1", genericRecord);

        SerializationException serializationException = assertThrows(SerializationException.class, () -> producerRunner.send(message));

        assertEquals("Error serializing Avro message", serializationException.getMessage());
        assertEquals("Not in union [\"null\",\"long\"]: aStringThatShouldBeALong (field=id)", serializationException.getCause().getMessage());
    }
}
