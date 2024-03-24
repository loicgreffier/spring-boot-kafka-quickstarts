package io.github.loicgreffier.consumer.retry.app;

import static io.github.loicgreffier.consumer.retry.constant.Topic.STRING_TOPIC;

import io.github.loicgreffier.consumer.retry.property.ConsumerProperties;
import io.github.loicgreffier.consumer.retry.service.ExternalService;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * This class represents a Kafka consumer runner that subscribes to a specific topic and
 * processes Kafka records.
 */
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

    /**
     * Asynchronously starts the Kafka consumer when the application is ready.
     * The asynchronous annotation is used to run the consumer in a separate thread and
     * not block the main thread.
     * The Kafka consumer processes messages from the STRING_TOPIC topic.
     * If an error occurs during the external system call, the consumer pauses the topic-partitions
     * and rewinds to the failed record offset.
     * As the consumer is paused, it will not commit the offsets and the next
     * call to poll() will not return any records. Consequently, the consumer
     * will honor the pause duration given by the poll() timeout.
     * Once the pause duration is elapsed, the consumer will resume the topic-partitions
     * and consume the records from the failed record offset.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            log.info("Subscribing to {} topic", STRING_TOPIC);

            consumer.subscribe(Collections.singleton(STRING_TOPIC),
                new CustomConsumerRebalanceListener(consumer, offsets));

            while (true) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
                log.info("Pulled {} records", messages.count());

                if (isPaused()) {
                    log.info("Consumer was paused, resuming topic-partitions {}",
                        consumer.assignment());
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

    /**
     * Checks if the consumer is paused.
     *
     * @return true if the consumer is paused, false otherwise.
     */
    private boolean isPaused() {
        return !consumer.paused().isEmpty();
    }

    /**
     * Rewinds the consumer the last saved offsets for each topic-partition.
     */
    private void rewind() {
        if (offsets.isEmpty()) {
            String autoOffsetReset = properties.getProperties()
                .getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                    OffsetResetStrategy.EARLIEST.name().toLowerCase());

            if (autoOffsetReset.equalsIgnoreCase(
                OffsetResetStrategy.EARLIEST.name().toLowerCase())) {
                consumer.seekToBeginning(consumer.assignment());
            } else if (autoOffsetReset.equalsIgnoreCase(
                OffsetResetStrategy.LATEST.name().toLowerCase())) {
                consumer.seekToEnd(consumer.assignment());
            }
        }

        log.info("Seeking to following partitions-offsets {}", offsets);

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            if (entry.getValue() != null) {
                consumer.seek(entry.getKey(), entry.getValue());
            } else {
                log.warn(
                    "Cannot rewind on {} to null offset, this could happen if the consumer "
                        + "group was just created", entry.getKey());
            }
        }
    }

    /**
     * Saves all the successfully processed records offsets by topic-partition.
     * It saves the offset of the next record to be processed in order to rewind
     * the consumer to the failed record offset in case of an external system error.
     *
     * @param message The message that was successfully processed.
     */
    private void updateOffsetsPosition(ConsumerRecord<String, String> message) {
        offsets.put(new TopicPartition(message.topic(), message.partition()),
            new OffsetAndMetadata(message.offset() + 1));
    }

    /**
     * Performs a synchronous commit of the consumed records.
     */
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
