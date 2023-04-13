package io.github.loicgreffier.streams.process.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import io.github.loicgreffier.streams.process.app.processor.PersonMetadataProcessor;
import io.github.loicgreffier.streams.process.constants.StateStore;
import io.github.loicgreffier.streams.process.constants.Topic;
import io.github.loicgreffier.streams.process.serdes.CustomSerdes;
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

import java.util.Collections;
import java.util.Set;

@Slf4j
public class KafkaStreamsProcessTopology {
    private KafkaStreamsProcessTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .process(new ProcessorSupplier<String, KafkaPerson, String, KafkaPersonMetadata>() {
                    @Override
                    public Processor<String, KafkaPerson, String, KafkaPersonMetadata> get() {
                        return new PersonMetadataProcessor();
                    }

                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        final StoreBuilder<KeyValueStore<String, Long>> countStoreBuilder = Stores
                                .keyValueStoreBuilder(Stores.persistentKeyValueStore(StateStore.PERSON_PROCESS_STATE_STORE.toString()),
                                        Serdes.String(), Serdes.Long());
                        return Collections.singleton(countStoreBuilder);
                    }
                })
                .to(Topic.PERSON_PROCESS_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
