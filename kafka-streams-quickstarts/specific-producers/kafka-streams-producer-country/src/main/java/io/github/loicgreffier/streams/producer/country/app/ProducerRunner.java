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
package io.github.loicgreffier.streams.producer.country.app;

import static io.github.loicgreffier.streams.producer.country.constant.Topic.COUNTRY_TOPIC;

import io.github.loicgreffier.avro.Country;
import io.github.loicgreffier.avro.CountryCode;
import java.util.List;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/** This class represents a Kafka producer runner that sends records to a specific topic. */
@Component
public class ProducerRunner {
    private static final Logger log = LoggerFactory.getLogger(ProducerRunner.class);
    private static final List<Country> COUNTRIES = List.of(
            buildCountry(CountryCode.FR, "France", "Paris", "French"),
            buildCountry(CountryCode.DE, "Germany", "Berlin", "German"),
            buildCountry(CountryCode.ES, "Spain", "Madrid", "Spanish"),
            buildCountry(CountryCode.IT, "Italy", "Rome", "Italian"),
            buildCountry(CountryCode.GB, "United Kingdom", "London", "English"),
            buildCountry(CountryCode.US, "United States", "Washington", "English"),
            buildCountry(CountryCode.BE, "Belgium", "Brussels", "French"));
    private final Producer<String, Country> producer;

    /**
     * Constructor.
     *
     * @param producer The Kafka producer
     */
    public ProducerRunner(Producer<String, Country> producer) {
        this.producer = producer;
    }

    /**
     * Asynchronously starts the Kafka producer when the application is ready.
     *
     * <p>The {@code @Async} annotation is used to run the producer in a separate thread, preventing it from blocking
     * the main thread.
     *
     * <p>The Kafka producer sends country records to the {@code COUNTRY_TOPIC}.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        for (Country country : COUNTRIES) {
            ProducerRecord<String, Country> message =
                    new ProducerRecord<>(COUNTRY_TOPIC, country.getCode().toString(), country);

            send(message);
        }
    }

    /**
     * Sends a message to the Kafka topic.
     *
     * @param message The message to send.
     */
    public void send(ProducerRecord<String, Country> message) {
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
    }

    /**
     * Builds a country of the referential.
     *
     * @param code The country code.
     * @param name The country name.
     * @param capital The country capital.
     * @param officialLanguage The country official language.
     * @return A country.
     */
    private static Country buildCountry(CountryCode code, String name, String capital, String officialLanguage) {
        return Country.newBuilder()
                .setCode(code)
                .setName(name)
                .setCapital(capital)
                .setOfficialLanguage(officialLanguage)
                .build();
    }
}
