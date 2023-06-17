package io.github.loicgreffier.consumer.avro.generic.app;

import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;

import static io.github.loicgreffier.consumer.avro.generic.constants.Topic.PERSON_TOPIC;

@Slf4j
@Component
@AllArgsConstructor
public class KafkaConsumerAvroGenericRunner implements ApplicationRunner {
    private final Consumer<String, GenericRecord> kafkaConsumer;

    @Override
    public void run(ApplicationArguments args) {
        try {
            log.info("Subscribing to {} topic", PERSON_TOPIC);

            kafkaConsumer.subscribe(Collections.singleton(PERSON_TOPIC), new KafkaConsumerAvroGenericRebalanceListener());

            while (true) {
                ConsumerRecords<String, GenericRecord> messages = kafkaConsumer.poll(Duration.ofMillis(1000));
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
