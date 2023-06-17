package io.github.loicgreffier.streams.process.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.nio.charset.StandardCharsets;

@Slf4j
public class PersonMetadataProcessor implements Processor<String, KafkaPerson, String, KafkaPersonMetadata> {
    private ProcessorContext<String, KafkaPersonMetadata> context;

    @Override
    public void init(ProcessorContext<String, KafkaPersonMetadata> context) {
        this.context = context;
    }

    @Override
    public void process(Record<String, KafkaPerson> message) {
        log.info("Received key = {}, value = {}", message.key(), message.value());

        RecordMetadata recordMetadata = context.recordMetadata().orElse(null);
        KafkaPersonMetadata newValue = KafkaPersonMetadata.newBuilder()
                .setPerson(message.value())
                .setTopic(recordMetadata != null ? recordMetadata.topic() : null)
                .setPartition(recordMetadata != null ? recordMetadata.partition() : null)
                .setOffset(recordMetadata != null ? recordMetadata.offset() : null)
                .build();

        message.headers().add("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        context.forward(message.withKey(message.value().getLastName()).withValue(newValue));
    }
}
