package io.github.loicgreffier.streams.processvalues.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 * This class represents a processor that adds metadata to the message.
 */
@Slf4j
public class PersonMetadataFixedKeyProcessor
    extends ContextualFixedKeyProcessor<String, KafkaPerson, KafkaPersonMetadata> {

    /**
     * Process the message by adding metadata to the message.
     * The message is then forwarded.
     *
     * @param message the message to process.
     */
    @Override
    public void process(FixedKeyRecord<String, KafkaPerson> message) {
        log.info("Received key = {}, value = {}", message.key(), message.value());

        Optional<RecordMetadata> recordMetadata = context().recordMetadata();
        KafkaPersonMetadata newValue = KafkaPersonMetadata.newBuilder()
            .setPerson(message.value())
            .setTopic(recordMetadata.map(RecordMetadata::topic).orElse(null))
            .setPartition(recordMetadata.map(RecordMetadata::partition).orElse(null))
            .setOffset(recordMetadata.map(RecordMetadata::offset).orElse(null))
            .build();

        message.headers().add("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        context().forward(message.withValue(newValue));
    }
}
