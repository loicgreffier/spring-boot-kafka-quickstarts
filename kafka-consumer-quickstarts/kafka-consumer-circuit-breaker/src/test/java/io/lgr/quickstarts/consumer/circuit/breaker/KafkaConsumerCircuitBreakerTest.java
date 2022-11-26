package io.lgr.quickstarts.consumer.circuit.breaker;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.consumer.circuit.breaker.app.KafkaConsumerCircuitBreakerRunner;
import io.lgr.quickstarts.consumer.circuit.breaker.constants.Topic;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class KafkaConsumerCircuitBreakerTest {
    private KafkaConsumerCircuitBreakerRunner consumerRunner;
    private MockConsumer<String, KafkaPerson> mockConsumer;
    private TopicPartition topicPartition;

    @BeforeEach
    void setUp() {
        topicPartition = new TopicPartition(Topic.PERSON_TOPIC.toString(), 0);
        mockConsumer = spy(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
        mockConsumer.schedulePollTask(() -> mockConsumer.rebalance(Collections.singletonList(topicPartition)));
        mockConsumer.updateBeginningOffsets(Map.of(topicPartition, 0L));
        mockConsumer.updateEndOffsets(Map.of(topicPartition, 0L));

        consumerRunner = new KafkaConsumerCircuitBreakerRunner(mockConsumer);
    }

    @Test
    void testConsumptionSuccess() {
        ConsumerRecord<String, KafkaPerson> message = new ConsumerRecord<>(Topic.PERSON_TOPIC.toString(), 0, 0, "1", KafkaPerson.newBuilder()
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
    void testCircuitBreaker() {
        ConsumerRecord<String, KafkaPerson> message = new ConsumerRecord<>(Topic.PERSON_TOPIC.toString(), 0, 0, "1", KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("First name")
                .setLastName("Last name")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build());

        ConsumerRecord<String, KafkaPerson> message2 = new ConsumerRecord<>(Topic.PERSON_TOPIC.toString(), 0, 2, "2", KafkaPerson.newBuilder()
                .setId(2L)
                .setFirstName("First name")
                .setLastName("Last name")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
                .build());

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message));

        mockConsumer.schedulePollTask(() -> {
            throw new RecordDeserializationException(topicPartition, 1, "Error deserializing", new Exception());
        });

        mockConsumer.schedulePollTask(() -> mockConsumer.addRecord(message2));

        mockConsumer.schedulePollTask(mockConsumer::wakeup);

        consumerRunner.run(new DefaultApplicationArguments());

        assertThat(mockConsumer.closed()).isTrue();
        verify(mockConsumer, times(5)).poll(any());
        verify(mockConsumer, times(2)).commitSync();
        verify(mockConsumer, times(1)).seek(topicPartition, 2);
    }
}
