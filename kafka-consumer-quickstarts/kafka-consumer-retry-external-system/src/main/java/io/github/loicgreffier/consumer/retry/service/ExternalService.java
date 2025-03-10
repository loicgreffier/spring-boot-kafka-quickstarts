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
package io.github.loicgreffier.consumer.retry.service;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/** This class represents an external service that is simulated to be called from the Kafka consumer. */
@Slf4j
@Service
public class ExternalService {
    private final Random random = new Random();

    @Value("${failureRate}")
    private int failureRate;

    /**
     * Simulates a call to an external system.
     *
     * @param message The Kafka ConsumerRecord that triggered the call.
     * @throws Exception if the external system call fails.
     */
    public void call(ConsumerRecord<String, String> message) throws Exception {
        int duration = random.nextInt(1000);

        log.info(
                "Simulating a call to an external system that will take {} for message {}", duration, message.offset());

        TimeUnit.MILLISECONDS.sleep(duration);
        if (random.nextInt(100) < failureRate) {
            throw new Exception("Call to external system failed");
        }
    }
}
