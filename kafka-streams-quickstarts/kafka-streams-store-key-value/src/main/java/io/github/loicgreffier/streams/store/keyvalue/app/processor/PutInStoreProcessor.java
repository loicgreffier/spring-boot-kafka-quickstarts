package io.github.loicgreffier.streams.store.keyvalue.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * This class represents a processor that puts the messages in a key-value store.
 */
@Slf4j
public class PutInStoreProcessor extends ContextualProcessor<String, KafkaPerson, String, KafkaPerson> {
    private final String storeName;
    private KeyValueStore<String, KafkaPerson> keyValueStore;

    /**
     * Constructor.
     *
     * @param storeName the name of the store.
     */
    public PutInStoreProcessor(String storeName) {
        this.storeName = storeName;
    }

    /**
     * Initialize the processor.
     *
     * @param context the processor context.
     */
    @Override
    public void init(ProcessorContext<String, KafkaPerson> context) {
        super.init(context);
        keyValueStore = context.getStateStore(storeName);
    }

    /**
     * Inserts the message in the state stores.
     *
     * @param message the message to process.
     */
    @Override
    public void process(Record<String, KafkaPerson> message) {
        log.info("Put key = {}, value = {} in store {}", message.key(), message.value(), storeName);
        keyValueStore.put(message.key(), message.value());
    }
}
