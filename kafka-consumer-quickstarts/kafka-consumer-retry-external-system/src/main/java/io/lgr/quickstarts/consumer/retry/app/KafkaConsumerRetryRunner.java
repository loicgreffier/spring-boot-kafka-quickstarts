package io.lgr.quickstarts.consumer.retry.app;

import io.lgr.quickstarts.consumer.retry.constants.Topic;
import io.lgr.quickstarts.consumer.retry.properties.ConsumerProperties;
import io.lgr.quickstarts.consumer.retry.services.ExternalService;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
@AllArgsConstructor
public class KafkaConsumerRetryRunner implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerRetryRunner.class);

    @Autowired
    private Consumer<String, String> kafkaConsumer;

    @Autowired
    private final ExternalService externalService;

    @Autowired
    private final ConsumerProperties consumerProperties;

    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    @Override
    public void run(ApplicationArguments args) {
        try {
            LOGGER.info("Subscribing to {} topic", Topic.STRING_TOPIC);

            kafkaConsumer.subscribe(Collections.singleton(Topic.STRING_TOPIC.toString()), new KafkaConsumerRetryRebalanceListener(kafkaConsumer, offsets));

            while (true) {
                ConsumerRecords<String, String> messages = kafkaConsumer.poll(Duration.ofMillis(1000));
                LOGGER.info("Pulled {} records", messages.count());

                if (isPaused()) {
                    LOGGER.info("Consumer was paused, resuming topic-partitions {}", kafkaConsumer.assignment());
                    kafkaConsumer.resume(kafkaConsumer.assignment());
                }

                for (ConsumerRecord<String, String> message : messages) {
                    LOGGER.info("Received offset = {}, partition = {}, key = {}, value = {}",
                            message.offset(), message.partition(), message.key(), message.value());

                    try {
                        // e.g. enrichment of a record with webservice, database...
                        externalService.call(message);
                    } catch (Exception e) {
                        LOGGER.error("Error during external system call", e);

                        LOGGER.info("Pausing topic-partitions {}", kafkaConsumer.assignment());
                        kafkaConsumer.pause(kafkaConsumer.assignment());
                        rewind();
                        break;
                    }

                    updateOffsetsPosition(message);
                }

                if (!messages.isEmpty() && !isPaused()) {
                    doCommitSync();
                }
            }
        } catch (WakeupException e) {
            LOGGER.info("Wake up signal received");
        } finally {
            LOGGER.info("Closing consumer");
            kafkaConsumer.close();
        }
    }

    private boolean isPaused() {
        return !kafkaConsumer.paused().isEmpty();
    }

    private void rewind() {
        if (offsets.isEmpty()) {
            String autoOffsetReset = consumerProperties.getProperties().
                    getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());

            if (autoOffsetReset.equalsIgnoreCase(OffsetResetStrategy.EARLIEST.name().toLowerCase())) {
                kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());
            } else if (autoOffsetReset.equalsIgnoreCase(OffsetResetStrategy.LATEST.name().toLowerCase())) {
                kafkaConsumer.seekToEnd(kafkaConsumer.assignment());
            }
        }

        LOGGER.info("Seeking to following partitions-offsets {}", offsets);

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            if (entry.getValue() != null) {
                kafkaConsumer.seek(entry.getKey(), entry.getValue());
            } else {
                LOGGER.warn("Cannot rewind on {} to null offset, this could happen if the consumer group was just created", entry.getKey());
            }
        }
    }

    private void updateOffsetsPosition(ConsumerRecord<String, String> message) {
        offsets.put(new TopicPartition(message.topic(), message.partition()), new OffsetAndMetadata(message.offset() + 1));
    }

    private void doCommitSync() {
        try {
            LOGGER.info("Committing the pulled records");
            kafkaConsumer.commitSync();
        } catch (WakeupException e) {
            LOGGER.info("Wake up signal received during commit process");
            doCommitSync();
            throw e;
        } catch (CommitFailedException e) {
            LOGGER.warn("Failed to commit", e);
        }
    }

    @PreDestroy
    public void preDestroy() {
        kafkaConsumer.wakeup();
    }
}
