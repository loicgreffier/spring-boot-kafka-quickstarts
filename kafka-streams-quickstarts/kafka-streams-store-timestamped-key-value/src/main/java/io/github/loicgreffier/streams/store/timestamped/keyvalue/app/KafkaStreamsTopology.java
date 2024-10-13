package io.github.loicgreffier.streams.store.timestamped.keyvalue.app;

import static io.github.loicgreffier.streams.store.timestamped.keyvalue.constant.StateStore.PERSON_TIMESTAMPED_KEY_VALUE_STORE;
import static io.github.loicgreffier.streams.store.timestamped.keyvalue.constant.StateStore.PERSON_TIMESTAMPED_KEY_VALUE_SUPPLIER_STORE;
import static io.github.loicgreffier.streams.store.timestamped.keyvalue.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.store.timestamped.keyvalue.app.processor.PutInStoreProcessor;
import io.github.loicgreffier.streams.store.timestamped.keyvalue.serdes.SerdesUtils;
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
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic and processes the records with
     * the {@link PutInStoreProcessor} processor that puts the records in a {@link KeyValueStore} state store.
     * It demonstrates the two strategies to use a state store in a processor:
     * - Using the {@link StreamsBuilder#addStateStore(StoreBuilder)} and specifying the store names
     * in the {@link org.apache.kafka.streams.kstream.KStream#process(ProcessorSupplier, String...)} method.
     * - Using the {@link ProcessorSupplier#stores()} method to attach the store to the topology and the processor.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        final StoreBuilder<TimestampedKeyValueStore<String, KafkaPerson>> keyValueStoreBuilder = Stores
            .timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(PERSON_TIMESTAMPED_KEY_VALUE_STORE),
                Serdes.String(), SerdesUtils.specificAvroValueSerdes()
            );

        streamsBuilder
            .addStateStore(keyValueStoreBuilder)
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .process(() -> new PutInStoreProcessor(keyValueStoreBuilder.name()), keyValueStoreBuilder.name());

        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC)
            .process(new ProcessorSupplier<String, KafkaPerson, String, KafkaPerson>() {
                @Override
                public Set<StoreBuilder<?>> stores() {
                    StoreBuilder<TimestampedKeyValueStore<String, KafkaPerson>> storeBuilder = Stores
                        .timestampedKeyValueStoreBuilder(
                            Stores.persistentTimestampedKeyValueStore(PERSON_TIMESTAMPED_KEY_VALUE_SUPPLIER_STORE),
                            Serdes.String(), SerdesUtils.specificAvroValueSerdes()
                        );

                    return Collections.singleton(storeBuilder);
                }

                @Override
                public Processor<String, KafkaPerson, String, KafkaPerson> get() {
                    return new PutInStoreProcessor(PERSON_TIMESTAMPED_KEY_VALUE_SUPPLIER_STORE);
                }
            });
    }
}
