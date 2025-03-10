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
package io.github.loicgreffier.streams.exception.handler.production.app;

import static io.github.loicgreffier.streams.exception.handler.production.constant.Topic.USER_PRODUCTION_EXCEPTION_HANDLER_TOPIC;
import static io.github.loicgreffier.streams.exception.handler.production.constant.Topic.USER_TOPIC;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.avro.KafkaUserWithEmail;
import io.github.loicgreffier.streams.exception.handler.production.serdes.SerdesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/** Kafka Streams topology. */
@Slf4j
public class KafkaStreamsTopology {
    private static final String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ";
    private static final int ONE_MEBIBYTE = 1048576;

    /**
     * Builds the Kafka Streams topology. The topology reads from the USER_TOPIC topic and either:
     *
     * <ul>
     *   <li>Populates the email field changing the record type from KafkaUser to KafkaUserWithEmail. As the email field
     *       is not nullable, it breaks the schema backward compatibility and triggers a serialization exception when
     *       registering the schema in the Schema Registry automatically.
     *   <li>Populates the biography field with a large text that exceeds the maximum record size allowed by Kafka
     *       (1MiB). It triggers a production exception due to the record being too large.
     * </ul>
     *
     * The population of the email field and the biography field is not triggered for all records to avoid generating
     * too many exceptions. The result is written to the USER_PRODUCTION_EXCEPTION_HANDLER_TOPIC topic.
     *
     * @param streamsBuilder The streams builder.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        StringBuilder stringBuilder = new StringBuilder();
        while (stringBuilder.toString().getBytes().length < ONE_MEBIBYTE) {
            stringBuilder.append(LOREM_IPSUM);
        }

        streamsBuilder.<String, KafkaUser>stream(
                        USER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, user) -> log.info("Received key = {}, value = {}", key, user))
                .mapValues(user -> {
                    if (user.getId() % 15 == 10) {
                        return KafkaUserWithEmail.newBuilder()
                                .setId(user.getId())
                                .setFirstName(user.getFirstName())
                                .setLastName(user.getLastName())
                                .setEmail(user.getFirstName() + "." + user.getLastName() + "@mail.com")
                                .setNationality(user.getNationality())
                                .setBirthDate(user.getBirthDate())
                                .setBiography(user.getBiography())
                                .build();
                    }

                    if (user.getId() % 15 == 1) {
                        user.setBiography(stringBuilder.toString());
                    }

                    return user;
                })
                .to(
                        USER_PRODUCTION_EXCEPTION_HANDLER_TOPIC,
                        Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
