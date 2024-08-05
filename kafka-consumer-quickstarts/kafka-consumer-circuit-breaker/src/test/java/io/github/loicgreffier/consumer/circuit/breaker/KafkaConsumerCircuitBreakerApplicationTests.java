package io.github.loicgreffier.consumer.circuit.breaker;

import static io.github.loicgreffier.consumer.circuit.breaker.constant.Topic.PERSON_TOPIC;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.consumer.circuit.breaker.app.ConsumerRunner;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
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

/**
 * This class contains unit tests for the Kafka consumer application.
 */
@ExtendWith(MockitoExtension.class)
class KafkaConsumerCircuitBreakerApplicationTests {
    @Spy
    private MockConsumer<String, KafkaPerson> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    @InjectMocks
    private ConsumerRunner consumerRunner;

    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition(PERSON_TOPIC, 0);
        mockConsumer.schedulePollTask(
            () -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));
    }

    @Test
    void shouldConsumeSuccessfully() {
        ConsumerRecord<String, KafkaPerson> message =
            new ConsumerRecord<>(PERSON_TOPIC, 0, 0, "1", KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("John")
                .setLastName("Doe")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build());

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());
        verify(mockConsumer).commitSync();
    }

    @Test
    void shouldBreakCircuitOnPoisonPill() {
        ConsumerRecord<String, KafkaPerson> message =
            new ConsumerRecord<>(PERSON_TOPIC, 0, 0, "1", KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("John")
                .setLastName("Doe")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build());

        ConsumerRecord<String, KafkaPerson> message2 =
            new ConsumerRecord<>(PERSON_TOPIC, 0, 2, "2", KafkaPerson.newBuilder()
                .setId(2L)
                .setFirstName("John")
                .setLastName("Doe")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build());

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));

        mockConsumer.schedulePollTask(() -> {
            throw new RecordDeserializationException(topicPartition, 1, "Error deserializing",
                new Exception());
        });

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message2));

        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());
        verify(mockConsumer, times(5)).poll(any());
        verify(mockConsumer, times(2)).commitSync();
        verify(mockConsumer).seek(topicPartition, 2);
    }
}
