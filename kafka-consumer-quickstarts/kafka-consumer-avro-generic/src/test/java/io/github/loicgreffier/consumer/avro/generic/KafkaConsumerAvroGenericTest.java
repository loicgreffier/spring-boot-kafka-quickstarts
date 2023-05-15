package io.github.loicgreffier.consumer.avro.generic;

import io.github.loicgreffier.consumer.avro.generic.app.KafkaConsumerAvroGenericRunner;
import io.github.loicgreffier.consumer.avro.generic.constants.Topic;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class KafkaConsumerAvroGenericTest {
    private KafkaConsumerAvroGenericRunner consumerRunner;
    private MockConsumer<String, GenericRecord> mockConsumer;
    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition(Topic.PERSON_TOPIC.toString(), 0);
        mockConsumer = spy(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));

        consumerRunner = new KafkaConsumerAvroGenericRunner(mockConsumer);
    }

    @Test
    void shouldConsumeSuccessfully() throws IOException {
        File schemaFile = new ClassPathResource("person.avsc").getFile();
        Schema schema = new Schema.Parser().parse(schemaFile);

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", 1L);
        genericRecord.put("firstName", "First name");
        genericRecord.put("lastName", "Last name");
        genericRecord.put("birthDate", Timestamp.from(Instant.parse("2000-01-01T01:00:00.00Z")).getTime());

        ConsumerRecord<String, GenericRecord> message = new ConsumerRecord<>(Topic.PERSON_TOPIC.toString(), 0, 0, "1", genericRecord);

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run(new DefaultApplicationArguments());

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(1)).commitSync();
    }

    @Test
    void shouldFailOnPoisonPill() throws IOException {
        File schemaFile = new ClassPathResource("person.avsc").getFile();
        Schema schema = new Schema.Parser().parse(schemaFile);

        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", 1L);
        genericRecord.put("firstName", "First name");
        genericRecord.put("lastName", "Last name");
        genericRecord.put("birthDate", Timestamp.from(Instant.parse("2000-01-01T01:00:00.00Z")).getTime());

        ConsumerRecord<String, GenericRecord> message = new ConsumerRecord<>(Topic.PERSON_TOPIC.toString(), 0, 0, "1", genericRecord);

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(() -> {
            throw new RecordDeserializationException(topicPartition, 1, "Error deserializing", new Exception());
        });
        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));

        assertThrows(RecordDeserializationException.class, () -> consumerRunner.run(new DefaultApplicationArguments()));

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(3)).poll(any());
        verify(mockConsumer, times(1)).commitSync();
    }
}
