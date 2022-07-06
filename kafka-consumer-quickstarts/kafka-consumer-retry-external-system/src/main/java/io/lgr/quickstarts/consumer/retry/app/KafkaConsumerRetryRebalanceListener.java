package io.lgr.quickstarts.consumer.retry.app;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

public class KafkaConsumerRetryRebalanceListener implements ConsumerRebalanceListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerRetryRebalanceListener.class);
    private final Consumer<String, String> consumer;
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public KafkaConsumerRetryRebalanceListener(Consumer<String, String> consumer, Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.consumer = consumer;
        this.offsets = offsets;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        LOGGER.info("Partition revoked : {}", partitions);

        for (TopicPartition topicPartition : partitions) {
            offsets.remove(topicPartition);
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        LOGGER.info("Partition assigned : {}", partitions);

        Map<TopicPartition, OffsetAndMetadata> offsetsTopicPartitions = consumer.committed(new HashSet<>(partitions));
        offsets.putAll(offsetsTopicPartitions.entrySet()
                .stream()
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        LOGGER.info("Partition lost : {}", partitions);

        ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}
