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

package io.github.loicgreffier.streams.average;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaAverageAge;
import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.streams.average.app.aggregator.AgeAggregator;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class AgeAggregatorTest {
    @Test
    void shouldAggregateAgeByNationality() {
        AgeAggregator aggregator = new AgeAggregator();
        KafkaAverageAge averageAge = new KafkaAverageAge(0L, 0L);

        LocalDate currentDate = LocalDate.now();

        KafkaUser userOne = KafkaUser.newBuilder()
            .setId(1L)
            .setFirstName("Bart")
            .setLastName("Simpson")
            .setNationality(CountryCode.US)
            .setBirthDate(currentDate.minusYears(25).atStartOfDay().toInstant(ZoneOffset.UTC)) // 25 years old
            .build();
        aggregator.apply(CountryCode.US.toString(), userOne, averageAge);

        KafkaUser userTwo = KafkaUser.newBuilder()
            .setId(2L)
            .setFirstName("Homer")
            .setLastName("Simpson")
            .setNationality(CountryCode.US)
            .setBirthDate(currentDate.minusYears(50).atStartOfDay().toInstant(ZoneOffset.UTC)) // 50 years old
            .build();
        aggregator.apply(CountryCode.US.toString(), userTwo, averageAge);

        KafkaUser userThree = KafkaUser.newBuilder()
            .setId(3L)
            .setFirstName("Abraham")
            .setLastName("Simpson")
            .setNationality(CountryCode.US)
            .setBirthDate(currentDate.minusYears(75).atStartOfDay().toInstant(ZoneOffset.UTC)) // 75 years old
            .build();
        aggregator.apply(CountryCode.US.toString(), userThree, averageAge);

        assertEquals(3L, averageAge.getCount());
        assertEquals(150L, averageAge.getAgeSum());
    }
}
