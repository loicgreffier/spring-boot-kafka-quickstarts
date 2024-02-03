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
     * @param fixedKeyRecord the message to process.
     */
    @Override
    public void process(FixedKeyRecord<String, KafkaPerson> fixedKeyRecord) {
        log.info("Received key = {}, value = {}", fixedKeyRecord.key(), fixedKeyRecord.value());

        Optional<RecordMetadata> recordMetadata = context().recordMetadata();
        KafkaPersonMetadata newValue = KafkaPersonMetadata.newBuilder()
            .setPerson(fixedKeyRecord.value())
            .setTopic(recordMetadata.map(RecordMetadata::topic).orElse(null))
            .setPartition(recordMetadata.map(RecordMetadata::partition).orElse(null))
            .setOffset(recordMetadata.map(RecordMetadata::offset).orElse(null))
            .build();

        fixedKeyRecord.headers().add("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        context().forward(fixedKeyRecord.withValue(newValue));
    }
}
