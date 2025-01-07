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

package io.github.loicgreffier.streams.map.app;

import static io.github.loicgreffier.streams.map.constant.Topic.PERSON_MAP_TOPIC;
import static io.github.loicgreffier.streams.map.constant.Topic.PERSON_TOPIC;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.map.serdes.SerdesUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology.
     * The topology reads from the PERSON_TOPIC topic, maps the first name and the last name to upper case and changes
     * the key to the last name.
     * The result is written to the PERSON_MAP_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person))
            .map((key, person) -> {
                person.setFirstName(person.getFirstName().toUpperCase());
                person.setLastName(person.getLastName().toUpperCase());
                return KeyValue.pair(person.getLastName(), person);
            })
            .to(PERSON_MAP_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
