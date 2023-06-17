package io.github.loicgreffier.streams.processvalues.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.nio.charset.StandardCharsets;

@Slf4j
public class PersonMetadataFixedKeyProcessor implements FixedKeyProcessor<String, KafkaPerson, KafkaPersonMetadata> {
    private FixedKeyProcessorContext<String, KafkaPersonMetadata> context;

    @Override
    public void init(FixedKeyProcessorContext<String, KafkaPersonMetadata> context) {
        this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, KafkaPerson> fixedKeyRecord) {
        log.info("Received key = {}, value = {}", fixedKeyRecord.key(), fixedKeyRecord.value());

        RecordMetadata recordMetadata = context.recordMetadata().orElse(null);
        KafkaPersonMetadata newValue = KafkaPersonMetadata.newBuilder()
                .setPerson(fixedKeyRecord.value())
                .setTopic(recordMetadata != null ? recordMetadata.topic() : null)
                .setPartition(recordMetadata != null ? recordMetadata.partition() : null)
                .setOffset(recordMetadata != null ? recordMetadata.offset() : null)
                .build();

        fixedKeyRecord.headers().add("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        context.forward(fixedKeyRecord.withValue(newValue));
    }
}
