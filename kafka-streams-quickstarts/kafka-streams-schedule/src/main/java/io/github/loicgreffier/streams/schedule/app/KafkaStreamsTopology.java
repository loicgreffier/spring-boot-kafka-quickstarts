package io.github.loicgreffier.streams.schedule.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.schedule.app.processor.CountNationalityProcessor;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

import java.util.Collections;
import java.util.Set;

import static io.github.loicgreffier.streams.schedule.constants.StateStore.PERSON_SCHEDULE_STATE_STORE;
import static io.github.loicgreffier.streams.schedule.constants.Topic.PERSON_SCHEDULE_TOPIC;
import static io.github.loicgreffier.streams.schedule.constants.Topic.PERSON_TOPIC;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .<String, KafkaPerson>stream(PERSON_TOPIC)
                .process(new ProcessorSupplier<String, KafkaPerson, String, Long>() {
                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        StoreBuilder<TimestampedKeyValueStore<String, Long>> storeBuilder = Stores
                                .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore(PERSON_SCHEDULE_STATE_STORE),
                                        Serdes.String(), Serdes.Long());
                        return Collections.singleton(storeBuilder);
                    }

                    @Override
                    public Processor<String, KafkaPerson, String, Long> get() {
                        return new CountNationalityProcessor();
                    }
                })
                .to(PERSON_SCHEDULE_TOPIC, Produced.valueSerde(Serdes.Long()));
    }
}
