package io.github.loicgreffier.streams.store.timestamped.keyvalue.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * This class represents a processor that puts the messages in a timestamped key-value store.
 */
@Slf4j
public class PutInStoreProcessor extends ContextualProcessor<String, KafkaPerson, String, KafkaPerson> {
    private final String storeName;
    private TimestampedKeyValueStore<String, KafkaPerson> timestampedKeyValueStore;

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
        timestampedKeyValueStore = context.getStateStore(storeName);
    }

    /**
     * Inserts the message in the state stores.
     *
     * @param message the message to process.
     */
    @Override
    public void process(Record<String, KafkaPerson> message) {
        log.info("Received key = {}, value = {}", message.key(), message.value());
        timestampedKeyValueStore.put(message.key(), ValueAndTimestamp.make(message.value(), message.timestamp()));
    }
}
