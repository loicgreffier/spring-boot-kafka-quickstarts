package io.github.loicgreffier.producer.avro.generic.app;

import static io.github.loicgreffier.producer.avro.generic.constant.Name.FIRST_NAMES;
import static io.github.loicgreffier.producer.avro.generic.constant.Name.LAST_NAMES;
import static io.github.loicgreffier.producer.avro.generic.constant.Topic.PERSON_TOPIC;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * This class represents a Kafka producer runner that sends records to a specific topic.
 */
@Slf4j
@Component
public class ProducerRunner {
    @Autowired
    private Producer<String, GenericRecord> producer;

    /**
     * Asynchronously starts the Kafka producer when the application is ready.
     * The asynchronous annotation is used to run the producer in a separate thread and
     * not block the main thread.
     * The Kafka producer produces generic Avro records to the PERSON_TOPIC topic.
     *
     * @throws IOException if the schema file cannot be read
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() throws IOException {
        File schemaFile = new ClassPathResource("person.avsc").getFile();
        Schema schema = new Schema.Parser().parse(schemaFile);

        int i = 0;
        while (true) {
            ProducerRecord<String, GenericRecord> message = new ProducerRecord<>(
                PERSON_TOPIC,
                String.valueOf(i),
                buildGenericRecord(schema, i)
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
    public Future<RecordMetadata> send(ProducerRecord<String, GenericRecord> message) {
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
     * Builds a generic Avro record.
     *
     * @param schema The Avro schema.
     * @param id     The record id.
     * @return The generic Avro record.
     */
    private GenericRecord buildGenericRecord(Schema schema, int id) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", (long) id);
        genericRecord.put("firstName", FIRST_NAMES[new Random().nextInt(FIRST_NAMES.length)]);
        genericRecord.put("lastName", LAST_NAMES[new Random().nextInt(LAST_NAMES.length)]);
        genericRecord.put("birthDate", System.currentTimeMillis());
        return genericRecord;
    }
}
