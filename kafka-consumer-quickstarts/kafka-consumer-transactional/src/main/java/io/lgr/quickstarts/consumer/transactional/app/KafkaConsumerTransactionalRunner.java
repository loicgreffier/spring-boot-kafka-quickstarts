package io.lgr.quickstarts.consumer.transactional.app;

import io.lgr.quickstarts.consumer.transactional.constants.Topic;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@AllArgsConstructor
public class KafkaConsumerTransactionalRunner implements ApplicationRunner {
    private final Consumer<String, String> kafkaConsumer;

    @Override
    public void run(ApplicationArguments args) {
        try {
            log.info("Subscribing to {} and {} topics", Topic.FIRST_STRING_TOPIC, Topic.SECOND_STRING_TOPIC);

            kafkaConsumer.subscribe(List.of(Topic.FIRST_STRING_TOPIC.toString(), Topic.SECOND_STRING_TOPIC.toString()),
                    new KafkaConsumerTransactionalRebalanceListener());

            while (true) {
                ConsumerRecords<String, String> messages = kafkaConsumer.poll(Duration.ofMillis(1000));
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
