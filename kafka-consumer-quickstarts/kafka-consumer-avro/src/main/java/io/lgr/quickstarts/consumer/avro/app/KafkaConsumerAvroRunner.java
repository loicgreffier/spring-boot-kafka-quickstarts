package io.lgr.quickstarts.consumer.avro.app;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.consumer.avro.constants.Topic;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;

@Component
@AllArgsConstructor
public class KafkaConsumerAvroRunner implements ApplicationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerAvroRunner.class);

    @Autowired
    private Consumer<String, KafkaPerson> kafkaConsumer;

    @Override
    public void run(ApplicationArguments args) {
        try {
            LOGGER.info("Subscribing to {} topic", Topic.PERSON_TOPIC);

            kafkaConsumer.subscribe(Collections.singleton(Topic.PERSON_TOPIC.toString()), new KafkaConsumerAvroRebalanceListener());

            while (true) {
                ConsumerRecords<String, KafkaPerson> messages = kafkaConsumer.poll(Duration.ofMillis(1000));
                LOGGER.info("Pulled {} records", messages.count());

                for (ConsumerRecord<String, KafkaPerson> message : messages) {
                    LOGGER.info("Received offset = {}, partition = {}, key = {}, id = {}, value = {}",
                            message.offset(), message.partition(), message.key(), message.value().getId(), message.value());
                }

                if (!messages.isEmpty()) {
                    doCommitSync();
                }
            }
        } catch (WakeupException e) {
            LOGGER.info("Wake up signal received");
        } finally {
            LOGGER.info("Closing consumer");
            kafkaConsumer.close();
        }
    }

    private void doCommitSync() {
        try {
            LOGGER.info("Committing the pulled records");
            kafkaConsumer.commitSync();
        } catch (WakeupException e) {
            LOGGER.info("Wake up signal received during commit process");
            doCommitSync();
            throw e;
        } catch (CommitFailedException e) {
            LOGGER.warn("Failed to commit", e);
        }
    }

    @PreDestroy
    public void preDestroy() {
        kafkaConsumer.wakeup();
    }
}
