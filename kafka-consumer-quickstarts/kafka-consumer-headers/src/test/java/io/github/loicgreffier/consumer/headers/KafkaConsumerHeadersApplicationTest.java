package io.github.loicgreffier.consumer.headers;

import static io.github.loicgreffier.consumer.headers.constant.Topic.STRING_TOPIC;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.loicgreffier.consumer.headers.app.ConsumerRunner;
import java.nio.charset.StandardCharsets;
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
class KafkaConsumerHeadersApplicationTest {
    @Spy
    private MockConsumer<String, String> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    @InjectMocks
    private ConsumerRunner consumerRunner;

    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition(STRING_TOPIC, 0);
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));
    }

    @Test
    void shouldConsumeSuccessfully() {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(STRING_TOPIC, 0, 0, "1", "John Doe");
        message.headers().add("id", "1".getBytes(StandardCharsets.UTF_8));
        message.headers().add("message", "John Doe".getBytes(StandardCharsets.UTF_8));

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

        assertTrue(mockConsumer.closed());

        verify(mockConsumer).commitSync();
    }

    @Test
    void shouldFailOnPoisonPill() {
        ConsumerRecord<String, String> message1 = new ConsumerRecord<>(STRING_TOPIC, 0, 0, "1", "John Doe");
        ConsumerRecord<String, String> message2 = new ConsumerRecord<>(STRING_TOPIC, 0, 2, "2", "Jane Smith");

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message1));
        mockConsumer.schedulePollTask(() -> {
            throw new RecordDeserializationException(topicPartition, 1, "Error deserializing",
                new Exception());
        });
        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message2));

        assertThrows(RecordDeserializationException.class, () -> consumerRunner.run());

        assertTrue(mockConsumer.closed());
        verify(mockConsumer, times(3)).poll(any());
        verify(mockConsumer).commitSync();
    }
}
