package io.github.loicgreffier.streams.store.window.timestamped.app;

import static io.github.loicgreffier.streams.store.window.timestamped.constant.StateStore.PERSON_TIMESTAMPED_WINDOW_STORE;
import static io.github.loicgreffier.streams.store.window.timestamped.constant.StateStore.PERSON_TIMESTAMPED_WINDOW_SUPPLIER_STORE;
import static io.github.loicgreffier.streams.store.window.timestamped.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.store.window.timestamped.app.processor.PutInStoreProcessor;
import io.github.loicgreffier.streams.store.window.timestamped.serdes.SerdesUtils;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStore;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic and processes the records with
     * the {@link PutInStoreProcessor} processor that puts the records in a {@link TimestampedWindowStore} state store.
     * It demonstrates the two strategies to use a state store in a processor:
     * - Using the {@link StreamsBuilder#addStateStore(StoreBuilder)} and specifying the store names
     * in the {@link org.apache.kafka.streams.kstream.KStream#process(ProcessorSupplier, String...)} method.
     * - Using the {@link ProcessorSupplier#stores()} method to attach the store to the topology and the processor.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        final StoreBuilder<TimestampedWindowStore<String, KafkaPerson>> storeBuilder = Stores
            .timestampedWindowStoreBuilder(
                Stores.persistentTimestampedWindowStore(
                    PERSON_TIMESTAMPED_WINDOW_STORE,
                    Duration.ofMinutes(10),
                    Duration.ofMinutes(5),
                    false
                ),
                Serdes.String(), SerdesUtils.getValueSerdes()
            );

        streamsBuilder
            .addStateStore(storeBuilder)
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .process(() -> new PutInStoreProcessor(storeBuilder.name()), storeBuilder.name());

        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .process(new ProcessorSupplier<String, KafkaPerson, String, KafkaPerson>() {
                @Override
                public Set<StoreBuilder<?>> stores() {
                    StoreBuilder<TimestampedWindowStore<String, KafkaPerson>> supplierStoreBuilder = Stores
                        .timestampedWindowStoreBuilder(
                            Stores.persistentTimestampedWindowStore(
                                PERSON_TIMESTAMPED_WINDOW_SUPPLIER_STORE,
                                Duration.ofMinutes(10),
                                Duration.ofMinutes(5),
                                false
                            ),
                            Serdes.String(), SerdesUtils.getValueSerdes()
                        );

                    return Collections.singleton(supplierStoreBuilder);
                }

                @Override
                public Processor<String, KafkaPerson, String, KafkaPerson> get() {
                    return new PutInStoreProcessor(PERSON_TIMESTAMPED_WINDOW_SUPPLIER_STORE);
                }
            });
    }
}
