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

import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaCountry;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * This class represents a Kafka producer runner that sends records to a specific topic.
 */
@Slf4j
@Component
public class ProducerRunner {
    private final Producer<String, KafkaCountry> producer;

    /**
     * Constructor.
     *
     * @param producer The Kafka producer
     */
    public ProducerRunner(Producer<String, KafkaCountry> producer) {
        this.producer = producer;
    }

    /**
     * Asynchronously starts the Kafka producer when the application is ready.
     * The asynchronous annotation is used to run the producer in a separate thread and
     * not block the main thread.
     * The Kafka producer produces country records to the COUNTRY_TOPIC topic.
     */
    @Async
    @EventListener(ApplicationReadyEvent.class)
    public void run() {
        for (KafkaCountry country : buildKafkaCountries()) {
            ProducerRecord<String, KafkaCountry> message = new ProducerRecord<>(
                COUNTRY_TOPIC,
                country.getCode().toString(),
                country
            );

            send(message);
        }
    }

    /**
     * Sends a message to the Kafka topic.
     *
     * @param message The message to send.
     */
    public void send(ProducerRecord<String, KafkaCountry> message) {
        producer.send(message, (recordMetadata, e) -> {
            if (e != null) {
                log.error(e.getMessage());
            } else {
                log.info("Success: topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                    recordMetadata.topic(),
                    recordMetadata.partition(),
                    recordMetadata.offset(),
                    message.key(),
                    message.value());
            }
        });
    }

    /**
     * Builds a list of Kafka countries.
     *
     * @return The list of Kafka countries.
     */
    private List<KafkaCountry> buildKafkaCountries() {
        KafkaCountry france = KafkaCountry.newBuilder()
            .setCode(CountryCode.FR)
            .setName("France")
            .setCapital("Paris")
            .setOfficialLanguage("French")
            .build();

        KafkaCountry germany = KafkaCountry.newBuilder()
            .setCode(CountryCode.DE)
            .setName("Germany")
            .setCapital("Berlin")
            .setOfficialLanguage("German")
            .build();

        KafkaCountry spain = KafkaCountry.newBuilder()
            .setCode(CountryCode.ES)
            .setName("Spain")
            .setCapital("Madrid")
            .setOfficialLanguage("Spanish")
            .build();

        KafkaCountry italy = KafkaCountry.newBuilder()
            .setCode(CountryCode.IT)
            .setName("Italy")
            .setCapital("Rome")
            .setOfficialLanguage("Italian")
            .build();

        KafkaCountry unitedKingdom = KafkaCountry.newBuilder()
            .setCode(CountryCode.GB)
            .setName("United Kingdom")
            .setCapital("London")
            .setOfficialLanguage("English")
            .build();

        KafkaCountry unitedStates = KafkaCountry.newBuilder()
            .setCode(CountryCode.US)
            .setName("United States")
            .setCapital("Washington")
            .setOfficialLanguage("English")
            .build();

        KafkaCountry belgium = KafkaCountry.newBuilder()
            .setCode(CountryCode.BE)
            .setName("Belgium")
            .setCapital("Brussels")
            .setOfficialLanguage("French")
            .build();

        return List.of(france, germany, spain, italy, unitedKingdom, unitedStates, belgium);
    }
}
