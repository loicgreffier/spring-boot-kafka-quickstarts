package io.github.loicgreffier.consumer.avro.generic.app;

import static io.github.loicgreffier.consumer.avro.generic.constant.Topic.PERSON_TOPIC;

import java.time.Duration;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
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
    private Consumer<String, GenericRecord> consumer;

    /**
     * Asynchronously starts the Kafka consumer when the application is ready.
     * The asynchronous annotation is used to run the consumer in a separate thread and
     * not block the main thread.
     * The Kafka consumer processes generic Avro records from the PERSON_TOPIC topic,
     * so it does not require to know the schema of the records.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            log.info("Subscribing to {} topic", PERSON_TOPIC);

            consumer.subscribe(Collections.singleton(PERSON_TOPIC), new CustomConsumerRebalanceListener());

            while (true) {
                ConsumerRecords<String, GenericRecord> messages = consumer.poll(Duration.ofMillis(1000));
                log.info("Pulled {} records", messages.count());

                for (ConsumerRecord<String, GenericRecord> message : messages) {
                    log.info("Received offset = {}, partition = {}, key = {}, firstName = {}, lastName = {}",
                        message.offset(),
                        message.partition(),
                        message.key(),
                        message.value().get("firstName"),
                        message.value().get("lastName"));
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
