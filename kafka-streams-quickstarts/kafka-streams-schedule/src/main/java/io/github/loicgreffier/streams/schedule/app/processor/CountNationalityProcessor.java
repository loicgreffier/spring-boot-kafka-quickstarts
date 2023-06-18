package io.github.loicgreffier.streams.schedule.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.schedule.constants.StateStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import static io.github.loicgreffier.streams.schedule.constants.StateStore.PERSON_SCHEDULE_STATE_STORE;

@Slf4j
public class CountNationalityProcessor implements Processor<String, KafkaPerson, String, Long> {
    private ProcessorContext<String, Long> context;
    private TimestampedKeyValueStore<String, Long> countNationalityStore;

    @Override
    public void init(ProcessorContext<String, Long> context) {
        this.context = context;
        this.countNationalityStore = context.getStateStore(PERSON_SCHEDULE_STATE_STORE);
        context.schedule(Duration.ofMinutes(2), PunctuationType.WALL_CLOCK_TIME, this::punctuateWallClockTime);
        context.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, this::punctuateStreamTime);
    }

    @Override
    public void process(Record<String, KafkaPerson> message) {
        log.info("Received key = {}, value = {}", message.key(), message.value());

        String key = message.value().getNationality().toString();
        ValueAndTimestamp<Long> count = countNationalityStore.putIfAbsent(key, ValueAndTimestamp.make(1L, Instant.now().getEpochSecond()));
        if (count != null) {
            countNationalityStore.put(key, ValueAndTimestamp.make(count.value() + 1, Instant.now().getEpochSecond()));
        }
    }

    private void punctuateWallClockTime(long timestamp) {
        try (KeyValueIterator<String, ValueAndTimestamp<Long>> iterator = countNationalityStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, ValueAndTimestamp<Long>> keyValue = iterator.next();
                countNationalityStore.put(keyValue.key, ValueAndTimestamp.make(0L, Instant.now().getEpochSecond()));
            }
        }

        log.info("All counters reset at {}", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp)));
    }

    private void punctuateStreamTime(long timestamp) {
        try (KeyValueIterator<String, ValueAndTimestamp<Long>> iterator = countNationalityStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<String, ValueAndTimestamp<Long>> keyValue = iterator.next();
                context.forward(new Record<>(keyValue.key, keyValue.value.value(), keyValue.value.timestamp()));
                log.info("{} persons of {} nationality at {}", keyValue.value.value(), keyValue.key,
                        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp)));
            }
        }
    }
}
