package io.github.loicgreffier.consumer.transaction.app;

import static io.github.loicgreffier.consumer.transaction.constants.Topic.FIRST_STRING_TOPIC;
import static io.github.loicgreffier.consumer.transaction.constants.Topic.SECOND_STRING_TOPIC;

import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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

    /**
     * Asynchronously starts the Kafka consumer when the application is ready.
     * The asynchronous annotation is used to run the consumer in a separate thread and
     * not block the main thread.
     * The Kafka consumer processes string records from
     * the FIRST_STRING_TOPIC and SECOND_STRING_TOPIC topics with an isolation level
     * of read_committed which means that it will only read records that are part of a committed
     * transaction or records that are not part of a transaction at all.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            log.info("Subscribing to {} and {} topics", FIRST_STRING_TOPIC, SECOND_STRING_TOPIC);

            consumer.subscribe(List.of(FIRST_STRING_TOPIC, SECOND_STRING_TOPIC),
                new CustomConsumerRebalanceListener());

            while (true) {
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
                log.info("Pulled {} records", messages.count());

                for (ConsumerRecord<String, String> message : messages) {
                    log.info("Received offset = {}, partition = {}, key = {}, value = {}",
                        message.offset(), message.partition(), message.key(), message.value());
                }

                if (!messages.isEmpty()) {
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
