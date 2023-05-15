package io.github.loicgreffier.consumer.retry;

import io.github.loicgreffier.consumer.retry.app.KafkaConsumerRetryRunner;
import io.github.loicgreffier.consumer.retry.constants.Topic;
import io.github.loicgreffier.consumer.retry.properties.ConsumerProperties;
import io.github.loicgreffier.consumer.retry.services.ExternalService;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.DefaultApplicationArguments;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class KafkaConsumerRetryTest {
    @Mock
    private ExternalService externalService;
    private AutoCloseable closeable;
    private KafkaConsumerRetryRunner consumerRunner;
    private MockConsumer<String, String> mockConsumer;
    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
        topicPartition = new TopicPartition(Topic.STRING_TOPIC.toString(), 0);
        mockConsumer = spy(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));

        ConsumerProperties consumerProperties = new ConsumerProperties();
        consumerProperties.getProperties().put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString());
        consumerRunner = new KafkaConsumerRetryRunner(mockConsumer, externalService, consumerProperties);
    }


    @AfterEach
    void tearDown() throws Exception {
        closeable.close();
    }

    @Test
    void shouldConsumeSuccessfully() {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(Topic.STRING_TOPIC.toString(), 0, 0, "1", "Message 1");

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run(new DefaultApplicationArguments());

        assertThat(mockConsumer.closed()).isTrue();

        verify(mockConsumer, times(1)).commitSync();
    }

    @Test
    void shouldRewindOffsetOnExternalSystemError() throws Exception {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(Topic.STRING_TOPIC.toString(), 0, 0, "1", "Message 1");
        ConsumerRecord<String, String> message2 = new ConsumerRecord<>(Topic.STRING_TOPIC.toString(), 0, 1, "2", "Message 2");
        ConsumerRecord<String, String> message3 = new ConsumerRecord<>(Topic.STRING_TOPIC.toString(), 0, 2, "3", "Message 3");

        // First poll to rewind, second poll to resume
        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> {
                mockConsumer.addRecord(message);
                mockConsumer.addRecord(message2);
                mockConsumer.addRecord(message3);
            });
        }

        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        doThrow(new Exception("Call to external system failed")).when(externalService).call(message2);

        consumerRunner.run(new DefaultApplicationArguments());

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(1)).pause(Collections.singleton(topicPartition));
        verify(mockConsumer, times(1)).seek(topicPartition, new OffsetAndMetadata(1));
        verify(mockConsumer, times(1)).resume(Collections.singleton(topicPartition));
    }

    @Test
    void shouldRewindToEarliestOnExternalSystemError() throws Exception {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(Topic.STRING_TOPIC.toString(), 0, 0, "1", "Message 1");
        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        }
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        doThrow(new Exception("Call to external system failed")).when(externalService).call(message);

        consumerRunner.run(new DefaultApplicationArguments());

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(1)).pause(Collections.singleton(topicPartition));
        verify(mockConsumer, times(1)).seekToBeginning(Collections.singleton(topicPartition));
        verify(mockConsumer, times(1)).resume(Collections.singleton(topicPartition));
    }

    @Test
    void shouldRewindToLatestOnExternalSystemError() throws Exception {
        ConsumerProperties consumerProperties = new ConsumerProperties();
        consumerProperties.getProperties().put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.LATEST.toString());
        consumerRunner = new KafkaConsumerRetryRunner(mockConsumer, externalService, consumerProperties);

        ConsumerRecord<String, String> message = new ConsumerRecord<>(Topic.STRING_TOPIC.toString(), 0, 0, "1", "Message 1");
        for (int i = 0; i < 2; i++) {
            mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        }
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        doThrow(new Exception("Call to external system failed")).when(externalService).call(message);

        consumerRunner.run(new DefaultApplicationArguments());

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(1)).pause(Collections.singleton(topicPartition));
        verify(mockConsumer, times(1)).seekToEnd(Collections.singleton(topicPartition));
        verify(mockConsumer, times(1)).resume(Collections.singleton(topicPartition));
    }
}