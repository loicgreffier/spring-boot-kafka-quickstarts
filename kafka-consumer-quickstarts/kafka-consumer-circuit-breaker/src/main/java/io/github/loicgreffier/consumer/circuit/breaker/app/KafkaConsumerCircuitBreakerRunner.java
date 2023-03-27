package io.github.loicgreffier.consumer.circuit.breaker.app;

import io.github.loicgreffier.consumer.circuit.breaker.constants.Topic;
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
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;

@Slf4j
@Component
@AllArgsConstructor
public class KafkaConsumerCircuitBreakerRunner implements ApplicationRunner {
    private final Consumer<String, KafkaPerson> kafkaConsumer;

    @Override
    public void run(ApplicationArguments args) {
        try {
            log.info("Subscribing to {} topic", Topic.PERSON_TOPIC);

            kafkaConsumer.subscribe(Collections.singleton(Topic.PERSON_TOPIC.toString()), new KafkaConsumerCircuitBreakerListener());

            while (true) {
                try {
                    ConsumerRecords<String, KafkaPerson> messages = kafkaConsumer.poll(Duration.ofMillis(1000));
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
                    kafkaConsumer.seek(e.topicPartition(), e.offset() + 1);
                }
            }
        } catch (WakeupException e) {
            log.info("Wake up signal received");
        } finally {
            log.info("Closing consumer");
            kafkaConsumer.close();
        }
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
