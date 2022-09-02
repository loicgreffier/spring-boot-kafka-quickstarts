package io.lgr.quickstarts.producer.transactional;

import io.lgr.quickstarts.producer.transactional.app.KafkaProducerTransactionalRunner;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;

import static io.lgr.quickstarts.producer.transactional.constants.Topic.FIRST_STRING_TOPIC;
import static io.lgr.quickstarts.producer.transactional.constants.Topic.SECOND_STRING_TOPIC;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaProducerTransactionalTest {
    private KafkaProducerTransactionalRunner producerRunner;
    private MockProducer<String, String> mockProducer;

    @Test
    void testTransactionAborted() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        producerRunner = new KafkaProducerTransactionalRunner(mockProducer);

        producerRunner.run(new DefaultApplicationArguments("--iteration=1"));

        assertThat(mockProducer.history()).isEmpty();
        assertThat(mockProducer.transactionInitialized()).isTrue();
        assertThat(mockProducer.transactionAborted()).isTrue();
        assertThat(mockProducer.transactionCommitted()).isFalse();
        assertThat(mockProducer.transactionInFlight()).isFalse();
    }

    @Test
    void testTransactionCommitted() {
        mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        producerRunner = new KafkaProducerTransactionalRunner(mockProducer);

        producerRunner.run(new DefaultApplicationArguments("--iteration=2"));

        assertThat(mockProducer.history()).hasSize(2);
        assertThat(mockProducer.history().get(0).topic()).isEqualTo(FIRST_STRING_TOPIC.toString());
        assertThat(mockProducer.history().get(0).value()).isEqualTo("Message 1");
        assertThat(mockProducer.history().get(1).topic()).isEqualTo(SECOND_STRING_TOPIC.toString());
        assertThat(mockProducer.history().get(1).value()).isEqualTo("Message 1");
        assertThat(mockProducer.transactionInitialized()).isTrue();
        assertThat(mockProducer.transactionCommitted()).isTrue();
        assertThat(mockProducer.transactionAborted()).isFalse();
        assertThat(mockProducer.transactionInFlight()).isFalse();
    }
}
