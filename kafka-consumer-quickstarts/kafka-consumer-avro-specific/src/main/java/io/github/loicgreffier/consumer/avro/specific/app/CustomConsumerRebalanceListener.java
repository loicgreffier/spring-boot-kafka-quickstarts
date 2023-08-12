package io.github.loicgreffier.consumer.avro.specific.app;

import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 * This class represents a custom Kafka consumer rebalance listener that handles partition
 * revocation, assignment, and loss events.
 */
@Slf4j
public class CustomConsumerRebalanceListener implements ConsumerRebalanceListener {
    /**
     * Called when partitions are revoked from the consumer.
     *
     * @param partitions The collection of revoked partitions.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partition revoked: {}", partitions);
    }

    /**
     * Called when partitions are assigned to the consumer.
     *
     * @param partitions The collection of assigned partitions.
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Partition assigned: {}", partitions);
    }

    /**
     * Called when partitions are lost by the consumer.
     *
     * @param partitions The collection of lost partitions.
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        log.info("Partition lost: {}", partitions);

        ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}
