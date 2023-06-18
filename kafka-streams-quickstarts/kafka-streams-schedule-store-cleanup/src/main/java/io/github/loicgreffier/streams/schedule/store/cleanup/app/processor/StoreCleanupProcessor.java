package io.github.loicgreffier.streams.schedule.store.cleanup.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.schedule.store.cleanup.constants.StateStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

import static io.github.loicgreffier.streams.schedule.store.cleanup.constants.StateStore.PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE;

@Slf4j
public class StoreCleanupProcessor implements Processor<String, KafkaPerson, String, KafkaPerson> {
    private ProcessorContext<String, KafkaPerson> context;
    private KeyValueStore<String, KafkaPerson> personStore;

    @Override
    public void init(ProcessorContext<String, KafkaPerson> context) {
        this.context = context;
        this.personStore = context.getStateStore(PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE);
        // Use stream time to avoid triggering unnecessary punctuation if no record comes
        context.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, this::punctuateCleanStore);
    }

    @Override
    public void process(Record<String, KafkaPerson> message) {
        if (message.value() == null) {
            log.info("Received tombstone for key = {}", message.key());
            personStore.delete(message.key());
            return;
        }

        log.info("Received key = {}, value = {}", message.key(), message.value());
        personStore.put(message.key(), message.value());
    }

    private void punctuateCleanStore(long timestamp) {
        log.info("Resetting {} store ", PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE);

        try (KeyValueIterator<String, KafkaPerson> iterator = personStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, KafkaPerson> keyValue = iterator.next();
                // Forward tombstone to input topic
                context.forward(new Record<>(keyValue.key, null, timestamp));
            }
        }
    }
}
