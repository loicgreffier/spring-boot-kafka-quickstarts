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

package io.github.loicgreffier.streams.aggregate.app.aggregator;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.kstream.Aggregator;

/**
 * This class represents an aggregator that aggregates the first names by last name.
 */
public class FirstNameByLastNameAggregator implements Aggregator<String, KafkaPerson, KafkaPersonGroup> {

    /**
     * Aggregates the first names by last name.
     *
     * @param key         The key of the record.
     * @param kafkaPerson The value of the record.
     * @param aggregate   The aggregate.
     * @return The updated aggregate.
     */
    @Override
    public KafkaPersonGroup apply(String key, KafkaPerson kafkaPerson, KafkaPersonGroup aggregate) {
        aggregate.getFirstNameByLastName().putIfAbsent(kafkaPerson.getLastName(), new ArrayList<>());

        List<String> firstNames = aggregate.getFirstNameByLastName().get(kafkaPerson.getLastName());
        firstNames.add(kafkaPerson.getFirstName());
        aggregate.getFirstNameByLastName().put(kafkaPerson.getLastName(), firstNames);

        return aggregate;
    }
}
