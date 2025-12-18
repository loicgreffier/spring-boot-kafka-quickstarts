/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.github.loicgreffier.streams.cogroup.app;

import static io.github.loicgreffier.streams.cogroup.constant.StateStore.USER_COGROUP_AGGREGATE_STORE;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.GROUP_USER_BY_LAST_NAME_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.GROUP_USER_BY_LAST_NAME_TOPIC_TWO;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.USER_COGROUP_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.USER_TOPIC;
import static io.github.loicgreffier.streams.cogroup.constant.Topic.USER_TOPIC_TWO;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.avro.KafkaUserAggregate;
import io.github.loicgreffier.streams.cogroup.app.aggregator.UserAggregator;
import io.github.loicgreffier.streams.cogroup.serdes.SerdesUtils;
import java.util.ArrayList;
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

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     *
     * <p>This topology reads records from the {@code USER_TOPIC} and {@code USER_TOPIC_TWO} topics. Both topics are
     * grouped by last name and cogrouped to combine the data from both streams. After cogrouping, the stream aggregates
     * the users by last name. The result of the aggregation is written to the {@code USER_COGROUP_TOPIC} topic.
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        final UserAggregator aggregator = new UserAggregator();

        final KGroupedStream<String, KafkaUser> groupedStreamOne = streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
                .groupBy(
                        (_, user) -> user.getLastName(),
                        Grouped.with(GROUP_USER_BY_LAST_NAME_TOPIC, Serdes.String(), SerdesUtils.getValueSerdes()));

        final KGroupedStream<String, KafkaUser> groupedStreamTwo = streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC_TWO, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
                .groupBy(
                        (_, user) -> user.getLastName(),
                        Grouped.with(GROUP_USER_BY_LAST_NAME_TOPIC_TWO, Serdes.String(), SerdesUtils.getValueSerdes()));

        groupedStreamOne
                .cogroup(aggregator)
                .cogroup(groupedStreamTwo, aggregator)
                .aggregate(
                        () -> new KafkaUserAggregate(new ArrayList<>()),
                        Materialized.<String, KafkaUserAggregate, KeyValueStore<Bytes, byte[]>>as(
                                        USER_COGROUP_AGGREGATE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesUtils.getValueSerdes()))
                .toStream()
                .to(USER_COGROUP_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
