package io.github.loicgreffier.consumer.avro.generic;

import io.github.loicgreffier.consumer.avro.generic.app.ConsumerRunner;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static io.github.loicgreffier.consumer.avro.generic.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerAvroGenericApplicationTests {
    @Spy
    private MockConsumer<String, GenericRecord> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    @InjectMocks
    private ConsumerRunner consumerRunner;

    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition(PERSON_TOPIC, 0);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));
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

        ConsumerRecord<String, GenericRecord> message = new ConsumerRecord<>(PERSON_TOPIC, 0, 0, "1", genericRecord);

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

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

        ConsumerRecord<String, GenericRecord> message = new ConsumerRecord<>(PERSON_TOPIC, 0, 0, "1", genericRecord);

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(() -> {
            throw new RecordDeserializationException(topicPartition, 1, "Error deserializing", new Exception());
        });
        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));

        assertThrows(RecordDeserializationException.class, () -> consumerRunner.run());

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(3)).poll(any());
        verify(mockConsumer, times(1)).commitSync();
    }
}
