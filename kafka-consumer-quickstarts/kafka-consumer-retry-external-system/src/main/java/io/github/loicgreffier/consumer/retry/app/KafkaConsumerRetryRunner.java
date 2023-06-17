package io.github.loicgreffier.consumer.retry.app;

import io.github.loicgreffier.consumer.retry.properties.KafkaConsumerProperties;
import io.github.loicgreffier.consumer.retry.services.ExternalService;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.github.loicgreffier.consumer.retry.constants.Topic.STRING_TOPIC;

@Slf4j
@Component
@AllArgsConstructor
public class KafkaConsumerRetryRunner implements ApplicationRunner {
    private final Consumer<String, String> kafkaConsumer;
    private final ExternalService externalService;
    private final KafkaConsumerProperties properties;
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    @Override
    public void run(ApplicationArguments args) {
        try {
            log.info("Subscribing to {} topic", STRING_TOPIC);

            kafkaConsumer.subscribe(Collections.singleton(STRING_TOPIC), new KafkaConsumerRetryRebalanceListener(kafkaConsumer, offsets));

            while (true) {
                ConsumerRecords<String, String> messages = kafkaConsumer.poll(Duration.ofMillis(1000));
                log.info("Pulled {} records", messages.count());

                if (isPaused()) {
                    log.info("Consumer was paused, resuming topic-partitions {}", kafkaConsumer.assignment());
                    kafkaConsumer.resume(kafkaConsumer.assignment());
                }

                for (ConsumerRecord<String, String> message : messages) {
                    log.info("Received offset = {}, partition = {}, key = {}, value = {}",
                            message.offset(), message.partition(), message.key(), message.value());

                    try {
                        // e.g. enrichment of a record with webservice, database...
                        externalService.call(message);
                    } catch (Exception e) {
                        log.error("Error during external system call", e);

                        log.info("Pausing topic-partitions {}", kafkaConsumer.assignment());
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
            log.info("Wake up signal received");
        } finally {
            log.info("Closing consumer");
            kafkaConsumer.close();
        }
    }

    private boolean isPaused() {
        return !kafkaConsumer.paused().isEmpty();
    }

    private void rewind() {
        if (offsets.isEmpty()) {
            String autoOffsetReset = properties.getProperties().
                    getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());

            if (autoOffsetReset.equalsIgnoreCase(OffsetResetStrategy.EARLIEST.name().toLowerCase())) {
                kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());
            } else if (autoOffsetReset.equalsIgnoreCase(OffsetResetStrategy.LATEST.name().toLowerCase())) {
                kafkaConsumer.seekToEnd(kafkaConsumer.assignment());
            }
        }

        log.info("Seeking to following partitions-offsets {}", offsets);

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            if (entry.getValue() != null) {
                kafkaConsumer.seek(entry.getKey(), entry.getValue());
            } else {
                log.warn("Cannot rewind on {} to null offset, this could happen if the consumer group was just created", entry.getKey());
            }
        }
    }

    private void updateOffsetsPosition(ConsumerRecord<String, String> message) {
        offsets.put(new TopicPartition(message.topic(), message.partition()), new OffsetAndMetadata(message.offset() + 1));
    }

    private void doCommitSync() {
        try {
            log.info("Committing the pulled records");
            kafkaConsumer.commitSync();
        } catch (WakeupException e) {
            log.info("Wake up signal received during commit process");
            doCommitSync();
            throw e;
        } catch (CommitFailedException e) {
            log.warn("Failed to commit", e);
        }
    }

    @PreDestroy
    public void preDestroy() {
        kafkaConsumer.wakeup();
    }
}
