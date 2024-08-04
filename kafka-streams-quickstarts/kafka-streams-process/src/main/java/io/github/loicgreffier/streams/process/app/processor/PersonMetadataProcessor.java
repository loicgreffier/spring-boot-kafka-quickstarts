package io.github.loicgreffier.streams.process.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 * This class represents a processor that adds metadata to the message
 * and changes the key.
 */
@Slf4j
public class PersonMetadataProcessor extends ContextualProcessor<String, KafkaPerson, String, KafkaPersonMetadata> {

    /**
     * Process the message by adding metadata to the message
     * and changing the key. The message is then forwarded.
     *
     * @param message the message to process.
     */
    @Override
    public void process(Record<String, KafkaPerson> message) {
        log.info("Received key = {}, value = {}", message.key(), message.value());

        Optional<RecordMetadata> recordMetadata = context().recordMetadata();
        KafkaPersonMetadata newValue = KafkaPersonMetadata.newBuilder()
            .setPerson(message.value())
            .setTopic(recordMetadata.map(RecordMetadata::topic).orElse(null))
            .setPartition(recordMetadata.map(RecordMetadata::partition).orElse(null))
            .setOffset(recordMetadata.map(RecordMetadata::offset).orElse(null))
            .build();

        message.headers().add("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        context().forward(message.withKey(message.value().getLastName()).withValue(newValue));
    }
}
