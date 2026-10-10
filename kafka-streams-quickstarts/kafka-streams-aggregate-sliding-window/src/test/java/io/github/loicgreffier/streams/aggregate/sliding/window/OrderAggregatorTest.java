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
package io.github.loicgreffier.streams.aggregate.sliding.window;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.OrderAggregate;
import io.github.loicgreffier.streams.aggregate.sliding.window.app.aggregator.OrderAggregator;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class OrderAggregatorTest {
    @Test
    void shouldAggregateOrdersByCustomer() {
        OrderAggregator aggregator = new OrderAggregator();
        OrderAggregate group = new OrderAggregate(new ArrayList<>());

        Order firstOrder = buildOrder(1L);
        aggregator.apply("1", firstOrder, group);

        Order secondOrder = buildOrder(2L);
        aggregator.apply("1", secondOrder, group);

        assertIterableEquals(List.of(firstOrder, secondOrder), group.getOrders());
    }

    private Order buildOrder(long id) {
        return Order.newBuilder()
                .setId(id)
                .setCustomerId(1L)
                .setItems(List.of("Laptop", "Mouse"))
                .setAmount(100.0)
                .build();
    }
}
