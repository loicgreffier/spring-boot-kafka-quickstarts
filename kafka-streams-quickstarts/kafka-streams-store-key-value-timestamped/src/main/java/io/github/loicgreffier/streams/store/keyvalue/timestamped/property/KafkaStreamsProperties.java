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
package io.github.loicgreffier.streams.store.keyvalue.timestamped.property;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** This class represents Kafka Streams properties configuration. */
@Getter
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaStreamsProperties {
    private final Map<String, String> properties = new HashMap<>();

    /**
     * Converts the Kafka Streams properties into a Java Properties object.
     *
     * @return A Properties object containing the Kafka Streams properties.
     */
    public Properties asProperties() {
        Properties config = new Properties();
        config.putAll(properties);
        return config;
    }
}
