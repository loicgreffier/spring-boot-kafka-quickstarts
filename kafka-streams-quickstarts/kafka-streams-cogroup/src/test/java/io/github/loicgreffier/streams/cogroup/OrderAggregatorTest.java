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
package io.github.loicgreffier.streams.cogroup;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.OrderAggregate;
import io.github.loicgreffier.streams.cogroup.app.aggregator.OrderAggregator;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class OrderAggregatorTest {
    @Test
    void shouldAggregateOrdersByCustomer() {
        OrderAggregator aggregator = new OrderAggregator();
        OrderAggregate group = new OrderAggregate(new ArrayList<>());

        Order orderOne = buildOrder("ORD1");
        aggregator.apply("CUS1", orderOne, group);

        Order orderTwo = buildOrder("ORD2");
        aggregator.apply("CUS1", orderTwo, group);

        assertIterableEquals(List.of(orderOne, orderTwo), group.getOrders());
    }

    private Order buildOrder(String id) {
        return Order.newBuilder()
                .setId(id)
                .setCustomerId("CUS1")
                .setItems(List.of("Laptop", "Mouse"))
                .setAmount(100.0)
                .build();
    }
}
