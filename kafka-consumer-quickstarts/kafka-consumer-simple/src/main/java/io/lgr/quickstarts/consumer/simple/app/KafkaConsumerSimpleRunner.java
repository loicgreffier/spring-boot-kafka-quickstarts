package io.lgr.quickstarts.consumer.simple.app;

import io.lgr.quickstarts.consumer.simple.constants.Topic;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;

@Slf4j
@Component
@AllArgsConstructor
public class KafkaConsumerSimpleRunner implements ApplicationRunner {
    private final Consumer<String, String> kafkaConsumer;

    @Override
    public void run(ApplicationArguments args) {
        try {
            log.info("Subscribing to {} topic", Topic.STRING_TOPIC);

            kafkaConsumer.subscribe(Collections.singleton(Topic.STRING_TOPIC.toString()), new KafkaConsumerSimpleRebalanceListener());

            while (true) {
                ConsumerRecords<String, String> messages = kafkaConsumer.poll(Duration.ofMillis(1000));
                log.info("Pulled {} records", messages.count());

                for (ConsumerRecord<String, String> message : messages) {
                    Header header = message.headers().lastHeader("headerKey");
                    String headerValue = header != null ? new String(header.value(), StandardCharsets.UTF_8) : "";

                    log.info("Received offset = {}, partition = {}, key = {}, value = {}, header = {}",
                            message.offset(), message.partition(), message.key(), message.value(), headerValue);
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
