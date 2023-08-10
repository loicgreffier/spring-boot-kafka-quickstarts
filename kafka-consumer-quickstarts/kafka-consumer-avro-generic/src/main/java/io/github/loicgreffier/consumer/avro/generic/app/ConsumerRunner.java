package io.github.loicgreffier.consumer.avro.generic.app;

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

import java.time.Duration;
import java.util.Collections;

import static io.github.loicgreffier.consumer.avro.generic.constants.Topic.PERSON_TOPIC;

@Slf4j
@Component
public class ConsumerRunner {
    @Autowired
    private Consumer<String, GenericRecord> consumer;

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
                    log.info("Received offset = {}, partition = {}, key = {}, value_firstName = {}, value_lastName = {}",
                            message.offset(), message.partition(), message.key(), message.value().get("firstName"),
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
