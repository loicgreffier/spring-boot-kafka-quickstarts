package io.github.loicgreffier.producer.transaction.app;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.github.loicgreffier.producer.transaction.constants.Topic.FIRST_STRING_TOPIC;
import static io.github.loicgreffier.producer.transaction.constants.Topic.SECOND_STRING_TOPIC;

@Slf4j
@Component
public class ProducerRunner {
    @Autowired
    private Producer<String, String> producer;

    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        log.info("Init transactions");
        producer.initTransactions();

        int i = 0;
        while (true) {
            ProducerRecord<String, String> firstMessage = new ProducerRecord<>(FIRST_STRING_TOPIC, String.valueOf(i), String.format("Message %s", i));
            ProducerRecord<String, String> secondMessage = new ProducerRecord<>(SECOND_STRING_TOPIC, String.valueOf(i), String.format("Message %s", i));

            sendInTransaction(Arrays.asList(firstMessage, secondMessage));

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                log.error("Interruption during sleep between message production", e);
                Thread.currentThread().interrupt();
            }

            i++;
        }
    }

    public final void sendInTransaction(List<ProducerRecord<String, String>> messages) {
        try {
            log.info("Begin transaction");
            producer.beginTransaction();

            messages.forEach(this::send);

            if (Integer.parseInt(messages.get(0).key()) % 3 == 0) {
                throw new Exception("Error during transaction...");
            }

            log.info("Commit transaction");
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            log.info("Closing producer");
            producer.close();
        } catch (Exception e) {
            log.info("Abort transaction", e);
            producer.abortTransaction();
        }
    }

    public Future<RecordMetadata> send(ProducerRecord<String, String> message) {
        return producer.send(message, ((recordMetadata, e) -> {
            if (e != null) {
                log.info("Fail: topic = {}, partition = {}, offset = {}, key = {}, value = {}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset(), message.key(), message.value());
                log.error(e.getMessage());
            } else {
                log.info("Success: topic = {}, partition = {}, offset = {}, key = {}, value = {}", recordMetadata.topic(),
                        recordMetadata.partition(), recordMetadata.offset(), message.key(), message.value());
            }
        }));
    }
}
