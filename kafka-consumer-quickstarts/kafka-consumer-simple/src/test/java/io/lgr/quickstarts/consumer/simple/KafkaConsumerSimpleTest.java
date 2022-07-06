package io.lgr.quickstarts.consumer.simple;

import io.lgr.quickstarts.consumer.simple.app.KafkaConsumerSimpleRunner;
import io.lgr.quickstarts.consumer.simple.constants.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class KafkaConsumerSimpleTest {
    private KafkaConsumerSimpleRunner consumerRunner;
    private MockConsumer<String, String> mockConsumer;
    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition(Topic.STRING_TOPIC.toString(), 0);
        mockConsumer = spy(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));

        consumerRunner = new KafkaConsumerSimpleRunner(mockConsumer);
    }

    @Test
    void testConsumptionSuccess() {
        ConsumerRecord<String, String> message = new ConsumerRecord<>(Topic.STRING_TOPIC.toString(), 0, 0, "1", "Message 1");
        message.headers().add("headerKey", "headerValue 1".getBytes(StandardCharsets.UTF_8));

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));
        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run(new DefaultApplicationArguments());

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(1)).commitSync();
    }

    @Test
    void testConsumptionFailedOnPoisonPill() {
        ConsumerRecord<String, String> message1 = new ConsumerRecord<>(Topic.STRING_TOPIC.toString(), 0, 0, "1", "Message 1");
        ConsumerRecord<String, String> message2 = new ConsumerRecord<>(Topic.STRING_TOPIC.toString(), 0, 2, "2", "Message 2");

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message1));
        mockConsumer.schedulePollTask(() -> {
            throw new RecordDeserializationException(topicPartition, 1, "Error deserializing", new Exception());
        });
        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message2));

        assertThrows(RecordDeserializationException.class, () -> consumerRunner.run(new DefaultApplicationArguments()));

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(3)).poll(any());
        verify(mockConsumer, times(1)).commitSync();
    }
}
