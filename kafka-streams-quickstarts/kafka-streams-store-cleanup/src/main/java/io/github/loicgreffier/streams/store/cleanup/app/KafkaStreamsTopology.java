package io.github.loicgreffier.streams.store.cleanup.app;

import static io.github.loicgreffier.streams.store.cleanup.constant.StateStore.PERSON_SCHEDULE_STORE_CLEANUP_STORE;
import static io.github.loicgreffier.streams.store.cleanup.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.store.cleanup.app.processor.StoreCleanupProcessor;
import io.github.loicgreffier.streams.store.cleanup.serdes.SerdesUtils;
import java.util.Collections;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic and processes the records with
     * the {@link StoreCleanupProcessor} processor. The state store is built before the processor is registered.
     * The result is written to the PERSON_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .process(new ProcessorSupplier<String, KafkaPerson, String, KafkaPerson>() {
                @Override
                public Set<StoreBuilder<?>> stores() {
                    StoreBuilder<KeyValueStore<String, KafkaPerson>> storeBuilder = Stores
                        .keyValueStoreBuilder(
                            Stores.persistentKeyValueStore(PERSON_SCHEDULE_STORE_CLEANUP_STORE),
                            Serdes.String(), SerdesUtils.specificAvroValueSerdes()
                        );

                    return Collections.singleton(storeBuilder);
                }

                @Override
                public Processor<String, KafkaPerson, String, KafkaPerson> get() {
                    return new StoreCleanupProcessor();
                }
            })
            .to(PERSON_TOPIC);
    }
}
