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
package io.github.loicgreffier.producer.avro.generic.app;

import static io.github.loicgreffier.producer.avro.generic.constant.Name.FIRST_NAMES;
import static io.github.loicgreffier.producer.avro.generic.constant.Name.LAST_NAMES;
import static io.github.loicgreffier.producer.avro.generic.constant.Topic.USER_TOPIC;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/** This class represents a Kafka producer runner that sends records to a specific topic. */
@Slf4j
@Component
public class ProducerRunner {
    private final Random random = new Random();
    private final Producer<String, GenericRecord> producer;

    @Setter
    private boolean stopped = false;

    /**
     * Constructor.
     *
     * @param producer The Kafka producer
     */
    public ProducerRunner(Producer<String, GenericRecord> producer) {
        this.producer = producer;
    }

    /**
     * Asynchronously starts the Kafka producer when the application is ready.
     *
     * <p>The asynchronous annotation is used to run the producer in a separate thread and not block the main thread.
     *
     * <p>The Kafka producer produces generic Avro records to the USER_TOPIC topic.
     *
     * @throws IOException if the schema file cannot be read
     * @throws InterruptedException if the thread is interrupted while sleeping
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() throws IOException, InterruptedException {
        File schemaFile = new ClassPathResource("user.avsc").getFile();
        Schema schema = new Schema.Parser().parse(schemaFile);

        int i = 0;
        while (!stopped) {
            ProducerRecord<String, GenericRecord> message =
                    new ProducerRecord<>(USER_TOPIC, String.valueOf(i), buildGenericRecord(schema, i));

            producer.send(message, (recordMetadata, e) -> {
                if (e != null) {
                    log.error(e.getMessage());
                } else {
                    log.info(
                            "Success: topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                            recordMetadata.topic(),
                            recordMetadata.partition(),
                            recordMetadata.offset(),
                            message.key(),
                            message.value());
                }
            });

            TimeUnit.SECONDS.sleep(1);

            i++;
        }
    }

    /**
     * Builds a generic Avro record.
     *
     * @param schema The Avro schema.
     * @param id The record id.
     * @return The generic Avro record.
     */
    private GenericRecord buildGenericRecord(Schema schema, int id) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("id", (long) id);
        genericRecord.put("firstName", FIRST_NAMES[random.nextInt(FIRST_NAMES.length)]);
        genericRecord.put("lastName", LAST_NAMES[random.nextInt(LAST_NAMES.length)]);
        genericRecord.put("birthDate", System.currentTimeMillis());
        return genericRecord;
    }
}
