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
package io.github.loicgreffier.streams.print.app;

import static io.github.loicgreffier.streams.print.constant.Topic.USER_TOPIC;
import static io.github.loicgreffier.streams.print.constant.Topic.USER_TOPIC_TWO;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.print.serdes.SerdesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {

    /**
     * Builds the Kafka Streams topology. The topology reads from the {@code USER_TOPIC} and writes the result to the
     * file specified by the {@code filePath} parameter. It also reads from the {@code USER_TOPIC_TWO} and prints the
     * result to the console.
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     * @param filePath The file path.
     */
    public static void topology(StreamsBuilder streamsBuilder, String filePath) {
        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
                .print(Printed.<String, KafkaUser>toFile(filePath)
                        .withKeyValueMapper(KafkaStreamsTopology::toOutput)
                        .withLabel(USER_TOPIC));

        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC_TWO, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .print(Printed.<String, KafkaUser>toSysOut()
                        .withKeyValueMapper(KafkaStreamsTopology::toOutput)
                        .withLabel(USER_TOPIC_TWO));
    }

    /**
     * Formats the key and value into a string.
     *
     * @param key The key.
     * @param kafkaUser The value.
     * @return The formatted string.
     */
    private static String toOutput(String key, KafkaUser kafkaUser) {
        return "Received key = %s, value = %s".formatted(key, kafkaUser);
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
