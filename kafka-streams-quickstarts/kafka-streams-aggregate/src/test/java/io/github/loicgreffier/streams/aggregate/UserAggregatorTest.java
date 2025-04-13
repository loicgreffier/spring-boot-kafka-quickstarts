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
package io.github.loicgreffier.streams.aggregate;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.github.loicgreffier.avro.KafkaUser;
import io.github.loicgreffier.avro.KafkaUserAggregate;
import io.github.loicgreffier.streams.aggregate.app.aggregator.UserAggregator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class UserAggregatorTest {
    @Test
    void shouldAggregateFirstNamesByLastName() {
        UserAggregator aggregator = new UserAggregator();
        KafkaUserAggregate group = new KafkaUserAggregate(new ArrayList<>());

        KafkaUser homer = buildKafkaUser("Homer");
        aggregator.apply("Simpson", homer, group);

        KafkaUser marge = buildKafkaUser("Marge");
        aggregator.apply("Simpson", marge, group);

        assertIterableEquals(List.of(homer, marge), group.getUsers());
    }

    private KafkaUser buildKafkaUser(String firstName) {
        return KafkaUser.newBuilder()
                .setId(1L)
                .setFirstName(firstName)
                .setLastName("Simpson")
                .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
                .build();
    }
}
