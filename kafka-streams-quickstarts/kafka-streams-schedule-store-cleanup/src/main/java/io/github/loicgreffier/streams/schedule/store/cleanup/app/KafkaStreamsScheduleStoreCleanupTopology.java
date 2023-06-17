package io.github.loicgreffier.streams.schedule.store.cleanup.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.schedule.store.cleanup.app.processor.StoreCleanupProcessor;
import io.github.loicgreffier.streams.schedule.store.cleanup.serdes.SerdesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import static io.github.loicgreffier.streams.schedule.store.cleanup.constants.StateStore.PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE;
import static io.github.loicgreffier.streams.schedule.store.cleanup.constants.Topic.PERSON_TOPIC;

@Slf4j
public class KafkaStreamsScheduleStoreCleanupTopology {
    private KafkaStreamsScheduleStoreCleanupTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        final StoreBuilder<KeyValueStore<String, KafkaPerson>> storeBuilder = Stores
                .keyValueStoreBuilder(Stores.persistentKeyValueStore(PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE),
                        Serdes.String(), SerdesUtils.specificAvroValueSerdes());

        KStream<String, KafkaPerson> personStream = streamsBuilder
                .addStateStore(storeBuilder)
                .stream(PERSON_TOPIC);

        personStream
                .process(StoreCleanupProcessor::new, PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE)
                .to(PERSON_TOPIC);
    }
}
