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

package io.github.loicgreffier.streams.branch.serdes;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import lombok.Setter;
import org.apache.avro.specific.SpecificRecord;

/**
 * Utility class for Serdes.
 */
public class SerdesUtils {
    @Setter
    private static Map<String, String> serdesConfig;

    /**
     * Create a SpecificAvroSerde for the value.
     *
     * @param <T> The type of the value.
     * @return The SpecificAvroSerde.
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getValueSerdes() {
        SpecificAvroSerde<T> serdes = new SpecificAvroSerde<>();
        serdes.configure(serdesConfig, false);
        return serdes;
    }

    /**
     * Private constructor.
     */
    private SerdesUtils() {}
}
