package io.github.loicgreffier.streams.cogroup.app;

import static io.github.loicgreffier.streams.cogroup.constant.StateStore.PERSON_COGROUP_AGGREGATE_STORE;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.GROUP_PERSON_BY_LAST_NAME_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.GROUP_PERSON_BY_LAST_NAME_TOPIC_TWO;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.PERSON_COGROUP_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.PERSON_TOPIC_TWO;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.cogroup.app.aggregator.FirstNameByLastNameAggregator;
import io.github.loicgreffier.streams.cogroup.serdes.SerdesUtils;
import java.util.HashMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic and the PERSON_TOPIC_TWO topic.
     * Both topics are grouped by last name and cogrouped. The cogrouped stream then aggregates
     * the first names by last name.
     * The result is written to the PERSON_COGROUP_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        final FirstNameByLastNameAggregator aggregator = new FirstNameByLastNameAggregator();

        final KGroupedStream<String, KafkaPerson> groupedStreamOne = streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .groupBy((key, person) -> person.getLastName(),
                Grouped.with(GROUP_PERSON_BY_LAST_NAME_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()));

        final KGroupedStream<String, KafkaPerson> groupedStreamTwo = streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC_TWO, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .groupBy((key, person) -> person.getLastName(),
                Grouped.with(GROUP_PERSON_BY_LAST_NAME_TOPIC_TWO, Serdes.String(), SerdesUtils.getValueSerdes()));

        groupedStreamOne
            .cogroup(aggregator)
            .cogroup(groupedStreamTwo, aggregator)
            .aggregate(
                () -> new KafkaPersonGroup(new HashMap<>()),
                Materialized
                    .<String, KafkaPersonGroup, KeyValueStore<Bytes, byte[]>>as(PERSON_COGROUP_AGGREGATE_STORE)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(SerdesUtils.getValueSerdes())
            )
            .toStream()
            .to(PERSON_COGROUP_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
