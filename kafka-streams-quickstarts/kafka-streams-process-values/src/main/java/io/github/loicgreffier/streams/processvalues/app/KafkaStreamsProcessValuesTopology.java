package io.github.loicgreffier.streams.processvalues.app;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.processvalues.app.processor.PersonMetadataFixedKeyProcessor;
import io.github.loicgreffier.streams.processvalues.constants.StateStore;
import io.github.loicgreffier.streams.processvalues.constants.Topic;
import io.github.loicgreffier.streams.processvalues.serdes.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

@Slf4j
public class KafkaStreamsProcessValuesTopology {
    private KafkaStreamsProcessValuesTopology() { }

    public static void topology(StreamsBuilder streamsBuilder) {
        final StoreBuilder<TimestampedKeyValueStore<String, Long>> countStoreBuilder = Stores
                .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore(StateStore.PERSON_PROCESS_VALUES_STATE_STORE.toString()),
                        Serdes.String(), Serdes.Long());

        streamsBuilder
                .addStateStore(countStoreBuilder)
                .stream(Topic.PERSON_TOPIC.toString(), Consumed.with(Serdes.String(), CustomSerdes.<KafkaPerson>getValueSerdes()))
                .processValues(PersonMetadataFixedKeyProcessor::new, StateStore.PERSON_PROCESS_VALUES_STATE_STORE.toString())
                .to(Topic.PERSON_PROCESS_VALUES_TOPIC.toString(), Produced.with(Serdes.String(), CustomSerdes.getValueSerdes()));
    }
}
