package io.github.loicgreffier.streams.process.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import io.github.loicgreffier.streams.process.constants.StateStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;

@Slf4j
public class KeyValueProcessor implements ProcessorSupplier<String, KafkaPerson, String, KafkaPersonMetadata> {
    @Override
    public Processor<String, KafkaPerson, String, KafkaPersonMetadata> get() {
        return new Processor<>() {
            private ProcessorContext<String, KafkaPersonMetadata> context;
            private KeyValueStore<String, Integer> countStore;

            @Override
            public void init(ProcessorContext<String, KafkaPersonMetadata> context) {
                this.context = context;
                this.countStore = context.getStateStore(StateStore.PERSON_PROCESS_STATE_STORE.toString());
            }

            @Override
            public void process(Record<String, KafkaPerson> message) {
                log.info("Processing key = {}, value = {}", message.key(), message.value());

                RecordMetadata recordMetadata = context.recordMetadata().orElse(null);

                String newKey = message.value().getLastName();
                KafkaPersonMetadata newValue = KafkaPersonMetadata.newBuilder()
                        .setPerson(message.value())
                        .setTopic(recordMetadata != null ? recordMetadata.topic() : null)
                        .setPartition(recordMetadata != null ? recordMetadata.partition() : null)
                        .setOffset(recordMetadata != null ? recordMetadata.offset() : null)
                        .build();

                Integer currentValue = countStore.get(newKey);
                if (currentValue == null) {
                    countStore.put(newKey, 1);
                } else {
                    countStore.put(newKey, currentValue + 1);
                }

                message.headers().add("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
                context.forward(message.withKey(message.value().getLastName()).withValue(newValue));
            }

            @Override
            public void close() {
                // Nothing to do here
            }
        };
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        final StoreBuilder<KeyValueStore<String, Integer>> countStoreBuilder = Stores
                        .keyValueStoreBuilder(Stores.persistentKeyValueStore(StateStore.PERSON_PROCESS_STATE_STORE.toString()),
                                Serdes.String(), Serdes.Integer());
        return Collections.singleton(countStoreBuilder);
    }
}
