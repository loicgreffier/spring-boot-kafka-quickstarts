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

package io.github.loicgreffier.streams.join.stream.table.app;

import io.github.loicgreffier.streams.join.stream.table.property.KafkaStreamsProperties;
import io.github.loicgreffier.streams.join.stream.table.serdes.SerdesUtils;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * This class represents a Kafka Streams runner that runs a topology.
 */
@Slf4j
@Component
public class KafkaStreamsRunner {
    private final ConfigurableApplicationContext applicationContext;
    private final KafkaStreamsProperties properties;
    private KafkaStreams kafkaStreams;

    /**
     * Constructor.
     *
     * @param applicationContext The application context
     * @param properties The Kafka Streams properties
     */
    public KafkaStreamsRunner(ConfigurableApplicationContext applicationContext, KafkaStreamsProperties properties) {
        this.applicationContext = applicationContext;
        this.properties = properties;
    }

    /**
     * Starts the Kafka Streams when the application is ready.
     * The Kafka Streams topology is built in the {@link KafkaStreamsTopology} class.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        SerdesUtils.setSerdesConfig(properties.getProperties());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KafkaStreamsTopology.topology(streamsBuilder);
        Topology topology = streamsBuilder.build();
        log.info("Topology description:\n {}", topology.describe());

        kafkaStreams = new KafkaStreams(topology, properties.asProperties());

        kafkaStreams.setUncaughtExceptionHandler(exception -> {
            log.error("A not covered exception occurred in {} Kafka Streams. Shutting down...",
                properties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG), exception);

            applicationContext.close();
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        kafkaStreams.setStateListener((newState, oldState) -> {
            if (newState.equals(KafkaStreams.State.ERROR)) {
                log.error("The {} Kafka Streams is in error state...",
                    properties.asProperties().get(StreamsConfig.APPLICATION_ID_CONFIG));

                applicationContext.close();
            }
        });

        kafkaStreams.start();
    }

    /**
     * Closes the Kafka Streams when the application is stopped.
     */
    @PreDestroy
    public void preDestroy() {
        log.info("Closing streams");
        kafkaStreams.close();
    }
}
