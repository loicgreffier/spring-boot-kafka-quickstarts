package io.github.loicgreffier.streams.process.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import io.github.loicgreffier.streams.process.constants.StateStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;

@Slf4j
public class PersonMetadataProcessor implements Processor<String, KafkaPerson, String, KafkaPersonMetadata> {
    private ProcessorContext<String, KafkaPersonMetadata> context;
    private KeyValueStore<String, Long> countStore;

    @Override
    public void init(ProcessorContext<String, KafkaPersonMetadata> context) {
        this.context = context;
        this.countStore = context.getStateStore(StateStore.PERSON_PROCESS_STATE_STORE.toString());
    }

    @Override
    public void process(Record<String, KafkaPerson> message) {
        log.info("Processing key = {}, value = {}", message.key(), message.value());

        String newKey = message.value().getLastName();
        Long currentValue = countStore.get(newKey);
        if (currentValue == null) {
            countStore.put(newKey, 1L);
        } else {
            countStore.put(newKey, currentValue + 1);
        }

        RecordMetadata recordMetadata = context.recordMetadata().orElse(null);
        KafkaPersonMetadata newValue = KafkaPersonMetadata.newBuilder()
                .setPerson(message.value())
                .setTopic(recordMetadata != null ? recordMetadata.topic() : null)
                .setPartition(recordMetadata != null ? recordMetadata.partition() : null)
                .setOffset(recordMetadata != null ? recordMetadata.offset() : null)
                .build();

        message.headers().add("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        context.forward(message.withKey(newKey).withValue(newValue));
    }

    @Override
    public void close() {
        // Nothing to do here
    }
}
