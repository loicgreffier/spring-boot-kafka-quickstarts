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
package io.github.loicgreffier.streams.flatmapvalues.app;

import static io.github.loicgreffier.streams.flatmapvalues.constant.Topic.USER_FLATMAP_VALUES_TOPIC;
import static io.github.loicgreffier.streams.flatmapvalues.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.flatmapvalues.serdes.SerdesUtils;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the {@code USER_TOPIC} topic, maps the value to a list
     * of strings containing the first name and last name. The resulting list of strings is written to the
     * {@code USER_FLATMAP_VALUES_TOPIC} topic.
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
                .flatMapValues(user -> Arrays.asList(user.getFirstName(), user.getLastName()))
                .to(USER_FLATMAP_VALUES_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
