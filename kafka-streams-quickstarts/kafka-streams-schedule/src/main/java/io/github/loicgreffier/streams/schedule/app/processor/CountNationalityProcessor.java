package io.github.loicgreffier.streams.schedule.app.processor;

import static io.github.loicgreffier.streams.schedule.constant.StateStore.PERSON_SCHEDULE_STORE;

import io.github.loicgreffier.avro.KafkaPerson;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * This class represents a processor that counts the number of persons by nationality.
 */
@Slf4j
public class CountNationalityProcessor extends ContextualProcessor<String, KafkaPerson, String, Long> {
    private KeyValueStore<String, Long> countNationalityStore;

    /**
     * Initialize the processor.
     * Opens the state store and schedules the punctuations. The first punctuation is scheduled on wall clock time and
     * resets the counters every 2 minutes. The second punctuation is scheduled on stream time and forwards the counters
     * to the output topic every minute. The stream time avoids triggering unnecessary punctuation if no record comes.
     *
     * @param context the processor context.
     */
    @Override
    public void init(ProcessorContext<String, Long> context) {
        super.init(context);
        countNationalityStore = context.getStateStore(PERSON_SCHEDULE_STORE);
        context.schedule(Duration.ofMinutes(2), PunctuationType.WALL_CLOCK_TIME, this::resetCounters);
        context.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, this::forwardCounters);
    }

    /**
     * For each message processed, increments the counter of the corresponding nationality in the state store.
     *
     * @param message the message to process.
     */
    @Override
    public void process(Record<String, KafkaPerson> message) {
        log.info("Received key = {}, value = {}", message.key(), message.value());

        String key = message.value().getNationality().toString();
        Long count = countNationalityStore.putIfAbsent(key, 1L);

        if (count != null) {
            countNationalityStore.put(key, count + 1);
        }
    }

    /**
     * For each entry in the state store, resets the counter to 0.
     *
     * @param timestamp the timestamp of the punctuation.
     */
    private void resetCounters(long timestamp) {
        try (KeyValueIterator<String, Long> iterator = countNationalityStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, Long> keyValue = iterator.next();
                countNationalityStore.put(keyValue.key, 0L);
            }
        }

        log.info("All counters reset at {}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp)));
    }

    /**
     * For each entry in the state store, forwards the counter to the output topic.
     *
     * @param timestamp the timestamp of the punctuation.
     */
    private void forwardCounters(long timestamp) {
        try (KeyValueIterator<String, Long> iterator = countNationalityStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, Long> keyValue = iterator.next();
                context().forward(new Record<>(keyValue.key, keyValue.value, timestamp));
                log.info("{} persons of {} nationality at {}",
                    keyValue.value,
                    keyValue.key,
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp))
                );
            }
        }
    }
}
