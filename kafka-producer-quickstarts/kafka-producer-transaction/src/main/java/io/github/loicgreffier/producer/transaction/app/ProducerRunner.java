package io.github.loicgreffier.producer.transaction.app;

import static io.github.loicgreffier.producer.transaction.constant.Topic.FIRST_STRING_TOPIC;
import static io.github.loicgreffier.producer.transaction.constant.Topic.SECOND_STRING_TOPIC;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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

/**
 * This class represents a Kafka producer runner that sends records to a specific topic.
 */
@Slf4j
@Component
public class ProducerRunner {

    @Autowired
    private Producer<String, String> producer;

    /**
     * Asynchronously starts the Kafka producer when the application is ready.
     * The asynchronous annotation is used to run the producer in a separate thread and
     * not block the main thread.
     * The Kafka producer produces two string records to two topics (FIRST_STRING_TOPIC and
     * SECOND_STRING_TOPIC) in a single transaction. Either both records are validated
     * by the transaction or both records are discarded.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        log.info("Init transactions");
        producer.initTransactions();

        int i = 0;
        while (true) {
            ProducerRecord<String, String> firstMessage =
                new ProducerRecord<>(FIRST_STRING_TOPIC, String.valueOf(i),
                    String.format("Message %s", i));
            ProducerRecord<String, String> secondMessage =
                new ProducerRecord<>(SECOND_STRING_TOPIC, String.valueOf(i),
                    String.format("Message %s", i));

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

    /**
     * Sends a list of messages to the Kafka topics in a single transaction.
     * If the first message key is a multiple of 3, the transaction is aborted.
     *
     * @param messages The messages to send.
     */
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


    /**
     * Sends a message to the Kafka topic.
     *
     * @param message The message to send.
     * @return A future of the record metadata.
     */
    public Future<RecordMetadata> send(ProducerRecord<String, String> message) {
        return producer.send(message, ((recordMetadata, e) -> {
            if (e != null) {
                log.error(e.getMessage()
                        + " (topic = {}, partition = {}, offset = {}, key = {}, value = {})",
                    recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
                    message.key(), message.value());
            } else {
                log.info("Success: topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                    recordMetadata.topic(),
                    recordMetadata.partition(), recordMetadata.offset(), message.key(),
                    message.value());
            }
        }));
    }
}
