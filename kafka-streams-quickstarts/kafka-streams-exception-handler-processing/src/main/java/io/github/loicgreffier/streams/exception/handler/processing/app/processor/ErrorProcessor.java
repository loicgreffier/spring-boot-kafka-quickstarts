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
package io.github.loicgreffier.streams.exception.handler.processing.app.processor;

import io.github.loicgreffier.avro.KafkaUser;
import java.time.Duration;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

/** This class represents a processor that throws an exception during processing and punctuation. */
public class ErrorProcessor extends ContextualFixedKeyProcessor<String, KafkaUser, KafkaUser> {

    /**
     * Initialize the processor.
     *
     * @param context The processor context.
     */
    @Override
    public void init(FixedKeyProcessorContext<String, KafkaUser> context) {
        super.init(context);
        context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
            throw new IllegalArgumentException("Forced processing exception during punctuation");
        });
    }

    /**
     * Process the record by throwing an exception if the first name or last name is null.
     *
     * @param message The record to process
     */
    @Override
    public void process(FixedKeyRecord<String, KafkaUser> message) {
        if (message.value().getFirstName() == null || message.value().getLastName() == null) {
            throw new IllegalArgumentException("First name and last name must not be null");
        }
        context().forward(message);
    }
}
