package io.github.loicgreffier.streams.producer.person.app;

import static io.github.loicgreffier.streams.producer.person.constant.Name.FIRST_NAMES;
import static io.github.loicgreffier.streams.producer.person.constant.Name.LAST_NAMES;
import static io.github.loicgreffier.streams.producer.person.constant.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.producer.person.constant.Topic.PERSON_TOPIC_TWO;

import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaPerson;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
     * The Kafka producer produces person records to two topics PERSON_TOPIC and PERSON_TOPIC_TWO.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        int i = 0;
        while (true) {
            ProducerRecord<String, KafkaPerson> messageOne = new ProducerRecord<>(
                PERSON_TOPIC,
                String.valueOf(i),
                buildKafkaPerson(i)
            );

            ProducerRecord<String, KafkaPerson> messageTwo = new ProducerRecord<>(
                PERSON_TOPIC_TWO,
                String.valueOf(i),
                buildKafkaPerson(i)
            );

            send(messageOne);
            send(messageTwo);

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
     */
    public void send(ProducerRecord<String, KafkaPerson> message) {
        producer.send(message, ((recordMetadata, e) -> {
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
        }));
    }

    /**
     * Builds a Kafka person.
     *
     * @param id The person id.
     * @return The Kafka person.
     */
    private KafkaPerson buildKafkaPerson(int id) {
        return KafkaPerson.newBuilder()
            .setId((long) id)
            .setFirstName(FIRST_NAMES[new Random().nextInt(FIRST_NAMES.length)])
            .setLastName(LAST_NAMES[new Random().nextInt(LAST_NAMES.length)])
            .setNationality(CountryCode.values()[new Random().nextInt(CountryCode.values().length)])
            .setBirthDate(Instant.ofEpochSecond(
                new Random().nextLong(Instant.parse("1924-01-01T00:00:00Z").getEpochSecond(),
                    Instant.now().getEpochSecond())))
            .build();
    }
}
