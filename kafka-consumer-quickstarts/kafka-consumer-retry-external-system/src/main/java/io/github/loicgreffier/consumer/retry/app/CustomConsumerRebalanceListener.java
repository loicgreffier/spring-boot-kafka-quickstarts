package io.github.loicgreffier.consumer.retry.app;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * This class represents a custom Kafka consumer rebalance listener that handles partition
 * revocation, assignment, and loss events.
 */
@Slf4j
@AllArgsConstructor
public class CustomConsumerRebalanceListener implements ConsumerRebalanceListener {
    private final Consumer<String, String> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    /**
     * Called when partitions are revoked from the consumer.
     *
     * @param partitions The collection of revoked partitions.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partition revoked : {}", partitions);

        for (TopicPartition topicPartition : partitions) {
            offsets.remove(topicPartition);
        }
    }

    /**
     * Called when partitions are assigned to the consumer.
     * For each assigned partition, it loads the last committed offset.
     *
     * @param partitions The collection of assigned partitions.
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Partition assigned : {}", partitions);

        Map<TopicPartition, OffsetAndMetadata> offsetsTopicPartitions =
            consumer.committed(new HashSet<>(partitions));
        offsets.putAll(offsetsTopicPartitions.entrySet()
            .stream()
            .filter(entry -> entry.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * Called when partitions are lost by the consumer.
     *
     * @param partitions The collection of lost partitions.
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        log.info("Partition lost : {}", partitions);

        ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}
