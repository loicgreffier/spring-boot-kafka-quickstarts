package io.lgr.quickstarts.consumer.circuit.breaker.app;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class KafkaConsumerCircuitBreakerListener implements ConsumerRebalanceListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerCircuitBreakerListener.class);

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOGGER.info("Partition revoked: {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOGGER.info("Partition assigned: {}", partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        LOGGER.info("Partition lost: {}", partitions);

        ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}
