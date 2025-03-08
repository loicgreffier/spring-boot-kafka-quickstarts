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

package io.github.loicgreffier.streams.filter.app;

import static io.github.loicgreffier.streams.filter.constant.Topic.USER_FILTER_TOPIC;
import static io.github.loicgreffier.streams.filter.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.filter.serdes.SerdesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Kafka Streams topology.
 */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the USER_TOPIC topic, filters by last name starting with "S"
     * and first name starting with "H".
     * The result is written to the USER_FILTER_TOPIC topic.
     *
     * @param streamsBuilder The streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaUser>stream(USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
            .filter((key, user) -> user.getLastName().startsWith("S"))
            .filterNot((key, user) -> !user.getFirstName().startsWith("H"))
            .to(USER_FILTER_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /**
     * Private constructor.
     */
    private KafkaStreamsTopology() {}
}
