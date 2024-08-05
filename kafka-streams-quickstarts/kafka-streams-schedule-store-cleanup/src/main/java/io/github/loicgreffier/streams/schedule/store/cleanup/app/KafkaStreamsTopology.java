package io.github.loicgreffier.streams.schedule.store.cleanup.app;

import static io.github.loicgreffier.streams.schedule.store.cleanup.constant.StateStore.PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE;
import static io.github.loicgreffier.streams.schedule.store.cleanup.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.schedule.store.cleanup.app.processor.StoreCleanupProcessor;
import io.github.loicgreffier.streams.schedule.store.cleanup.serdes.SerdesUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * This class represents a Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the PERSON_TOPIC topic
     * and processes the records with the {@link StoreCleanupProcessor} processor.
     * The state store is built before the processor is registered.
     * The result is written to the PERSON_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        final StoreBuilder<KeyValueStore<String, KafkaPerson>> storeBuilder = Stores
            .keyValueStoreBuilder(
                Stores.persistentKeyValueStore(PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE),
                Serdes.String(), SerdesUtils.specificAvroValueSerdes()
            );

        streamsBuilder
            .addStateStore(storeBuilder)
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .process(StoreCleanupProcessor::new, PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE)
            .to(PERSON_TOPIC);
    }
}
