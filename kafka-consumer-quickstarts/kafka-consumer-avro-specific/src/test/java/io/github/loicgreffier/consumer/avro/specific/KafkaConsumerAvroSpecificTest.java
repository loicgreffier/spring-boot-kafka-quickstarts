package io.github.loicgreffier.consumer.avro.specific;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.consumer.avro.specific.app.KafkaConsumerAvroSpecificRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static io.github.loicgreffier.consumer.avro.specific.constants.Topic.PERSON_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class KafkaConsumerAvroSpecificTest {
    private KafkaConsumerAvroSpecificRunner consumerRunner;
    private MockConsumer<String, KafkaPerson> mockConsumer;
    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition(PERSON_TOPIC, 0);
        mockConsumer = spy(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));

        consumerRunner = new KafkaConsumerAvroSpecificRunner(mockConsumer);
    }

    @Test
    void shouldConsumeSuccessfully() {
        ConsumerRecord<String, KafkaPerson> message = new ConsumerRecord<>(PERSON_TOPIC, 0, 0, "1", KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("First name")
                .setLastName("Last name")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build());

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run(new DefaultApplicationArguments());

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(1)).commitSync();
    }

    @Test
    void shouldFailOnPoisonPill() {
        ConsumerRecord<String, KafkaPerson> message = new ConsumerRecord<>(PERSON_TOPIC, 0, 0, "1", KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("First name")
                .setLastName("Last name")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build());

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
