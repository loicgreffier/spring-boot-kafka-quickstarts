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

package io.github.loicgreffier.streams.merge.app;

import static io.github.loicgreffier.streams.merge.constant.Topic.PERSON_MERGE_TOPIC;
import static io.github.loicgreffier.streams.merge.constant.Topic.PERSON_TOPIC;
import static io.github.loicgreffier.streams.merge.constant.Topic.PERSON_TOPIC_TWO;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.merge.serdes.SerdesUtils;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Kafka Streams topology.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the PERSON_TOPIC topic
     * and the PERSON_TOPIC_TWO topic, then merge the two streams into the
     * PERSON_MERGE_TOPIC topic.
     *
     * @param streamsBuilder the streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        KStream<String, KafkaPerson> streamOne = streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person));

        KStream<String, KafkaPerson> streamTwo = streamsBuilder
            .<String, KafkaPerson>stream(PERSON_TOPIC_TWO, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
            .peek((key, person) -> log.info("Received key = {}, value = {}", key, person));

        streamOne
            .merge(streamTwo)
            .to(PERSON_MERGE_TOPIC, Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }
}
