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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.app.aggregator.FirstNameByLastNameAggregator;
import java.time.Instant;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

class FirstNameByLastNameAggregatorTest {
    @Test
    void shouldAggregateFirstNamesByLastName() {
        FirstNameByLastNameAggregator aggregator = new FirstNameByLastNameAggregator();
        KafkaPersonGroup group = new KafkaPersonGroup(new HashMap<>());

        aggregator.apply("Simpson", KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName("Homer")
            .setLastName("Simpson")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build(), group);

        aggregator.apply("Simpson", KafkaPerson.newBuilder()
            .setId(2L)
            .setFirstName("Marge")
            .setLastName("Simpson")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build(), group);

        aggregator.apply("Van Houten", KafkaPerson.newBuilder()
            .setId(3L)
            .setFirstName("Milhouse")
            .setLastName("Van Houten")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build(), group);

        assertTrue(group.getFirstNameByLastName().containsKey("Simpson"));
        assertEquals(2, group.getFirstNameByLastName().get("Simpson").size());
        assertEquals("Homer", group.getFirstNameByLastName().get("Simpson").get(0));
        assertEquals("Marge", group.getFirstNameByLastName().get("Simpson").get(1));

        assertTrue(group.getFirstNameByLastName().containsKey("Van Houten"));
        assertEquals(1, group.getFirstNameByLastName().get("Van Houten").size());
        assertEquals("Milhouse", group.getFirstNameByLastName().get("Van Houten").getFirst());
    }
}
