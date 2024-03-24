package io.github.loicgreffier.consumer.retry;

import static io.github.loicgreffier.consumer.retry.constant.Topic.STRING_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.loicgreffier.consumer.retry.app.ConsumerRunner;
import io.github.loicgreffier.consumer.retry.property.ConsumerProperties;
import io.github.loicgreffier.consumer.retry.service.ExternalService;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * This class contains unit tests for the Kafka consumer application.
 */
@ExtendWith(MockitoExtension.class)
class KafkaConsumerRetryApplicationTests {
    @Spy
    private MockConsumer<String, String> mockConsumer =
        new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    @Mock
    private ExternalService externalService;

    @Mock
    private ConsumerProperties properties;

    @InjectMocks
    private ConsumerRunner consumerRunner;

    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition(STRING_TOPIC, 0);
        mockConsumer.schedulePollTask(
            () -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));
    }

    @Test
    void shouldConsumeSuccessfully() {
        ConsumerRecord<String, String> message =
            new ConsumerRecord<>(STRING_TOPIC, 0, 0, "1", "John Doe");

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run();

        assertThat(mockConsumer.closed()).isTrue();

        verify(mockConsumer, times(1)).commitSync();
    }

    @Test
    void shouldRewindOffsetOnExternalSystemError() throws Exception {
        ConsumerRecord<String, String> message =
            new ConsumerRecord<>(STRING_TOPIC, 0, 0, "1", "John Doe");
        ConsumerRecord<String, String> message2 =
            new ConsumerRecord<>(STRING_TOPIC, 0, 1, "2", "Jane Smith");
        ConsumerRecord<String, String> message3 =
            new ConsumerRecord<>(STRING_TOPIC, 0, 2, "3", "Robert Williams");

        // First poll to rewind, second poll to resume
        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> {
                mockConsumer.addRecord(message);
                mockConsumer.addRecord(message2);
                mockConsumer.addRecord(message3);
            });
        }

        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        // Throw exception for message 2, otherwise do nothing
        // Should read from message 2 on second poll loop
        doNothing().when(externalService).call(argThat(arg -> !arg.equals(message2)));
        doThrow(new Exception("Call to external system failed"))
            .when(externalService).call(message2);

        consumerRunner.run();

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(1)).pause(Collections.singleton(topicPartition));
        verify(mockConsumer, times(1)).seek(topicPartition, new OffsetAndMetadata(1));
        verify(mockConsumer, times(1)).resume(Collections.singleton(topicPartition));
    }

    @Test
    void shouldRewindToEarliestOnExternalSystemError() throws Exception {
        ConsumerRecord<String, String> message =
            new ConsumerRecord<>(STRING_TOPIC, 0, 0, "1", "John Doe");

        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        }
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        doThrow(new Exception("Call to external system failed")).when(externalService)
            .call(message);

        consumerRunner.run();

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(1)).pause(Collections.singleton(topicPartition));
        verify(mockConsumer, times(1)).seekToBeginning(Collections.singleton(topicPartition));
        verify(mockConsumer, times(1)).resume(Collections.singleton(topicPartition));
    }

    @Test
    void shouldRewindToLatestOnExternalSystemError() throws Exception {
        ConsumerRecord<String, String> message =
            new ConsumerRecord<>(STRING_TOPIC, 0, 0, "1", "John Doe");

        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        }
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        when(properties.getProperties()).thenReturn(
            Map.of(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString()));
        doThrow(new Exception("Call to external system failed")).when(externalService)
            .call(message);

        consumerRunner.run();

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(1)).pause(Collections.singleton(topicPartition));
        verify(mockConsumer, times(1)).seekToEnd(Collections.singleton(topicPartition));
        verify(mockConsumer, times(1)).resume(Collections.singleton(topicPartition));
    }
}
