package io.github.loicgreffier.consumer.circuit.breaker.app;

import io.github.loicgreffier.avro.KafkaPerson;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;

import static io.github.loicgreffier.consumer.circuit.breaker.constants.Topic.PERSON_TOPIC;

@Slf4j
@Component
public class ConsumerRunner {
    @Autowired
    private Consumer<String, KafkaPerson> consumer;

    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        try {
            log.info("Subscribing to {} topic", PERSON_TOPIC);

            consumer.subscribe(Collections.singleton(PERSON_TOPIC), new CustomConsumerRebalanceListener());

            while (true) {
                try {
                    ConsumerRecords<String, KafkaPerson> messages = consumer.poll(Duration.ofMillis(1000));
                    log.info("Pulled {} records", messages.count());

                    for (ConsumerRecord<String, KafkaPerson> message : messages) {
                        log.info("Received offset = {}, partition = {}, key = {}, value = {}",
                                message.offset(), message.partition(), message.key(), message.value());
                    }

                    if (!messages.isEmpty()) {
                        doCommitSync();
                    }
                } catch (RecordDeserializationException e) {
                    log.info("Error while deserializing message from topic-partition {}-{} at offset {}. Seeking to the next offset {}",
                            e.topicPartition().topic(), e.topicPartition().partition(), e.offset(), e.offset() + 1);
                    consumer.seek(e.topicPartition(), e.offset() + 1);
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
