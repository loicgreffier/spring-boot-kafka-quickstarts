package io.github.loicgreffier.producer.avro.specific.app;

import static io.github.loicgreffier.producer.avro.specific.constant.Name.FIRST_NAMES;
import static io.github.loicgreffier.producer.avro.specific.constant.Name.LAST_NAMES;
import static io.github.loicgreffier.producer.avro.specific.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
    private Producer<String, KafkaPerson> producer;

    /**
     * Asynchronously starts the Kafka producer when the application is ready.
     * The asynchronous annotation is used to run the producer in a separate thread and
     * not block the main thread.
     * The Kafka producer produces specific Avro records to the PERSON_TOPIC topic.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        int i = 0;
        while (true) {
            ProducerRecord<String, KafkaPerson> message = new ProducerRecord<>(
                PERSON_TOPIC,
                String.valueOf(i),
                buildKafkaPerson(i)
            );

            send(message);

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
     * Sends a message to the Kafka topic.
     *
     * @param message The message to send.
     * @return A future of the record metadata.
     */
    public Future<RecordMetadata> send(ProducerRecord<String, KafkaPerson> message) {
        return producer.send(message, (recordMetadata, e) -> {
            if (e != null) {
                log.error(e.getMessage());
            } else {
                log.info("Success: topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                    recordMetadata.topic(),
                    recordMetadata.partition(),
                    recordMetadata.offset(),
                    message.key(),
                    message.value());
            }
        });
    }

    /**
     * Builds a specific Avro record.
     *
     * @param id The record id.
     * @return The specific Avro record.
     */
    private KafkaPerson buildKafkaPerson(int id) {
        return KafkaPerson.newBuilder()
            .setId((long) id)
            .setFirstName(FIRST_NAMES[new Random().nextInt(FIRST_NAMES.length)])
            .setLastName(LAST_NAMES[new Random().nextInt(LAST_NAMES.length)])
            .setBirthDate(Instant.now())
            .build();
    }
}
