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

package io.github.loicgreffier.streams.processvalues.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonMetadata;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.RecordMetadata;

/**
 * This class represents a processor that adds metadata to the message.
 */
@Slf4j
public class PersonMetadataFixedKeyProcessor
    extends ContextualFixedKeyProcessor<String, KafkaPerson, KafkaPersonMetadata> {

    /**
     * Process the message by adding metadata to the message.
     * The message is then forwarded.
     *
     * @param message the message to process.
     */
    @Override
    public void process(FixedKeyRecord<String, KafkaPerson> message) {
        log.info("Received key = {}, value = {}", message.key(), message.value());

        Optional<RecordMetadata> recordMetadata = context().recordMetadata();
        KafkaPersonMetadata newValue = KafkaPersonMetadata.newBuilder()
            .setPerson(message.value())
            .setTopic(recordMetadata.map(RecordMetadata::topic).orElse(null))
            .setPartition(recordMetadata.map(RecordMetadata::partition).orElse(null))
            .setOffset(recordMetadata.map(RecordMetadata::offset).orElse(null))
            .build();

        message.headers().add("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8));
        context().forward(message.withValue(newValue));
    }
}
