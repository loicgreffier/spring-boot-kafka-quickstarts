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

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.OrderAverageAmount;
import io.github.loicgreffier.streams.average.app.aggregator.AmountAggregator;
import java.util.List;
import org.junit.jupiter.api.Test;

class AmountAggregatorTest {
    @Test
    void shouldAggregateAmountByCustomer() {
        AmountAggregator aggregator = new AmountAggregator();
        OrderAverageAmount averageAmount = new OrderAverageAmount(0L, 0.0);

        aggregator.apply("1", buildOrder(1L, 25.0), averageAmount);
        aggregator.apply("1", buildOrder(2L, 50.0), averageAmount);
        aggregator.apply("1", buildOrder(3L, 75.0), averageAmount);

        assertEquals(3L, averageAmount.getCount());
        assertEquals(150.0, averageAmount.getAmountSum());
    }

    private Order buildOrder(long id, double amount) {
        return Order.newBuilder()
                .setId(id)
                .setCustomerId(1L)
                .setItems(List.of("Laptop", "Mouse"))
                .setAmount(amount)
                .build();
    }
}
