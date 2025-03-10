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
package io.github.loicgreffier.consumer.headers.config;

import io.github.loicgreffier.consumer.headers.property.ConsumerProperties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** This class provides configuration for creating a Kafka consumer instance. */
@Configuration
public class ConsumerConfig {
    /**
     * Creates a Kafka consumer instance using the specified properties. When the application is stopped, the consumer
     * bean is automatically destroyed. When the consumer bean is destroyed, the {@link Consumer#wakeup()} method is
     * called instead of {@link Consumer#close()} to ensure thread safety.
     *
     * @param properties The consumer properties to configure the Kafka consumer.
     * @return A Kafka consumer instance.
     */
    @Bean(destroyMethod = "wakeup")
    public Consumer<String, String> kafkaConsumer(ConsumerProperties properties) {
        return new KafkaConsumer<>(properties.asProperties());
    }
}
