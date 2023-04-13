package io.github.loicgreffier.streams.processvalues.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import io.github.loicgreffier.streams.processvalues.constants.StateStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Slf4j
public class PersonMetadataFixedKeyProcessor implements FixedKeyProcessor<String, KafkaPerson, KafkaPersonMetadata> {
    private FixedKeyProcessorContext<String, KafkaPersonMetadata> context;
    private TimestampedKeyValueStore<String, Long> countStore;

    @Override
    public void init(FixedKeyProcessorContext<String, KafkaPersonMetadata> context) {
        this.context = context;
        this.countStore = context.getStateStore(StateStore.PERSON_PROCESS_VALUES_STATE_STORE.toString());
    }

    @Override
    public void process(FixedKeyRecord<String, KafkaPerson> fixedKeyRecord) {
        log.info("Processing key = {}, value = {}", fixedKeyRecord.key(), fixedKeyRecord.value());

        String lastName = fixedKeyRecord.value().getLastName();
        ValueAndTimestamp<Long> currentValue = countStore.get(lastName);
        if (currentValue == null) {
            countStore.put(lastName, ValueAndTimestamp.make(1L, Instant.now().toEpochMilli()));
        } else {
            countStore.put(lastName, ValueAndTimestamp.make(currentValue.value() + 1, Instant.now().toEpochMilli()));
        }

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
