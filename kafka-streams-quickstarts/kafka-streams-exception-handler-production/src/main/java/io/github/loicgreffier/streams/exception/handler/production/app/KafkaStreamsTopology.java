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

import static io.github.loicgreffier.streams.exception.handler.production.constant.Topic.ORDER_PRODUCTION_EXCEPTION_HANDLER_TOPIC;
import static io.github.loicgreffier.streams.exception.handler.production.constant.Topic.ORDER_TOPIC;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.OrderWithCurrency;
import io.github.loicgreffier.streams.exception.handler.production.serdes.SerdesUtils;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Kafka Streams topology. */
public class KafkaStreamsTopology {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsTopology.class);
    private static final String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. ";
    private static final int ONE_MEBIBYTE = 1048576;

    /**
     * Builds the Kafka Streams topology.
     *
     * <p>This topology reads from the {@code ORDER_TOPIC} topic and either:
     *
     * <ul>
     *   <li>Populates the currency field, changing the record type from {@link Order} to {@link OrderWithCurrency}.
     *       Since the currency field is non-nullable, this breaks schema backward compatibility, triggering a
     *       serialization exception when registering the schema in the Schema Registry automatically.
     *   <li>Replaces the items with a large text that exceeds the maximum record size allowed by Kafka (1 MiB),
     *       triggering a production exception due to the record being too large.
     * </ul>
     *
     * The population of the currency and items fields is not applied to all records in order to avoid generating too
     * many exceptions. Serialization and production exceptions are handled by the produce exception handler. The result
     * is written to the {@code ORDER_PRODUCTION_EXCEPTION_HANDLER_TOPIC} topic.
     *
     * @param streamsBuilder The {@link StreamsBuilder} used to build the Kafka Streams topology.
     */
    public static void topology(StreamsBuilder streamsBuilder) {
        StringBuilder stringBuilder = new StringBuilder();
        while (stringBuilder.toString().getBytes().length < ONE_MEBIBYTE) {
            stringBuilder.append(LOREM_IPSUM);
        }

        streamsBuilder.<String, Order>stream(ORDER_TOPIC, Consumed.with(Serdes.String(), SerdesUtils.getValueSerdes()))
                .peek((key, order) -> log.info("Processing key = {}, value = {}", key, order))
                .mapValues(order -> {
                    if (order.getId() % 15 == 10) {
                        return OrderWithCurrency.newBuilder()
                                .setId(order.getId())
                                .setCustomerId(order.getCustomerId())
                                .setItems(order.getItems())
                                .setAmount(order.getAmount())
                                .setCurrency("EUR")
                                .build();
                    }

                    if (order.getId() % 15 == 1) {
                        order.setItems(List.of(stringBuilder.toString()));
                    }

                    return order;
                })
                .to(
                        ORDER_PRODUCTION_EXCEPTION_HANDLER_TOPIC,
                        Produced.with(Serdes.String(), SerdesUtils.getValueSerdes()));
    }

    /** Private constructor. */
    private KafkaStreamsTopology() {}
}
