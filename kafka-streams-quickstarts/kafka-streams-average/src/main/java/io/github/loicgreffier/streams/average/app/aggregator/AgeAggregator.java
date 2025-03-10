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
package io.github.loicgreffier.streams.average.app.aggregator;

import io.github.loicgreffier.avro.KafkaAverageAge;
import io.github.loicgreffier.avro.KafkaUser;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import org.apache.kafka.streams.kstream.Aggregator;

/** This class represents an aggregator that aggregates the age. */
public class AgeAggregator implements Aggregator<String, KafkaUser, KafkaAverageAge> {
    /**
     * Aggregates the age.
     *
     * @param key The key of the record.
     * @param kafkaUser The value of the record.
     * @param aggregate The aggregate.
     * @return The updated aggregate.
     */
    @Override
    public KafkaAverageAge apply(String key, KafkaUser kafkaUser, KafkaAverageAge aggregate) {
        aggregate.setCount(aggregate.getCount() + 1);

        LocalDate currentDate = LocalDate.now();
        LocalDate birthDate = LocalDate.ofInstant(kafkaUser.getBirthDate(), ZoneOffset.UTC);
        aggregate.setAgeSum(
                aggregate.getAgeSum() + Period.between(birthDate, currentDate).getYears());
        return aggregate;
    }
}
