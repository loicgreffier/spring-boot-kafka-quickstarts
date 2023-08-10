package io.github.loicgreffier.consumer.retry.app;

import io.github.loicgreffier.consumer.retry.properties.ConsumerProperties;
import io.github.loicgreffier.consumer.retry.services.ExternalService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.github.loicgreffier.consumer.retry.constants.Topic.STRING_TOPIC;

@Slf4j
@Component
public class ConsumerRunner {
    @Autowired
    private Consumer<String, String> consumer;

    @Autowired
    private ExternalService externalService;

    @Autowired
    private ConsumerProperties properties;

    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            log.info("Subscribing to {} topic", STRING_TOPIC);

            consumer.subscribe(Collections.singleton(STRING_TOPIC), new CustomConsumerRebalanceListener(consumer, offsets));

            while (true) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
                log.info("Pulled {} records", messages.count());

                if (isPaused()) {
                    log.info("Consumer was paused, resuming topic-partitions {}", consumer.assignment());
                    consumer.resume(consumer.assignment());
                }

                for (ConsumerRecord<String, String> message : messages) {
                    log.info("Received offset = {}, partition = {}, key = {}, value = {}",
                            message.offset(), message.partition(), message.key(), message.value());

                    try {
                        // e.g. enrichment of a record with webservice, database...
                        externalService.call(message);
                    } catch (Exception e) {
                        log.error("Error during external system call", e);

                        log.info("Pausing topic-partitions {}", consumer.assignment());
                        consumer.pause(consumer.assignment());
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
            consumer.close();
        }
    }

    private boolean isPaused() {
        return !consumer.paused().isEmpty();
    }

    private void rewind() {
        if (offsets.isEmpty()) {
            String autoOffsetReset = properties.getProperties().
                    getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());

            if (autoOffsetReset.equalsIgnoreCase(OffsetResetStrategy.EARLIEST.name().toLowerCase())) {
                consumer.seekToBeginning(consumer.assignment());
            } else if (autoOffsetReset.equalsIgnoreCase(OffsetResetStrategy.LATEST.name().toLowerCase())) {
                consumer.seekToEnd(consumer.assignment());
            }
        }

        log.info("Seeking to following partitions-offsets {}", offsets);

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            if (entry.getValue() != null) {
                consumer.seek(entry.getKey(), entry.getValue());
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
            consumer.commitSync();
        } catch (WakeupException e) {
            log.info("Wake up signal received during commit process");
            doCommitSync();
            throw e;
        } catch (CommitFailedException e) {
            log.warn("Failed to commit", e);
        }
    }
}
