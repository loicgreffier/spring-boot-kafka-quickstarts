package io.github.loicgreffier.streams.schedule.app;

import static io.github.loicgreffier.streams.schedule.constant.StateStore.PERSON_SCHEDULE_STORE;
import static io.github.loicgreffier.streams.schedule.constant.Topic.PERSON_SCHEDULE_TOPIC;
import static io.github.loicgreffier.streams.schedule.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.schedule.app.processor.CountNationalityProcessor;
import io.github.loicgreffier.streams.schedule.serdes.SerdesUtils;
import java.util.Collections;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
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
     * the {@link CountNationalityProcessor} processor. The processor supplier registers
     * a {@link KeyValueStore} state store when it is built.
     * The result is written to the PERSON_SCHEDULE_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .process(new ProcessorSupplier<String, KafkaPerson, String, Long>() {
                @Override
                public Set<StoreBuilder<?>> stores() {
                    StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores
                        .keyValueStoreBuilder(
                            Stores.persistentKeyValueStore(PERSON_SCHEDULE_STORE),
                            Serdes.String(), Serdes.Long()
                        );

                    return Collections.singleton(storeBuilder);
                }

                @Override
                public Processor<String, KafkaPerson, String, Long> get() {
                    return new CountNationalityProcessor();
                }
            })
            .to(PERSON_SCHEDULE_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }
}
