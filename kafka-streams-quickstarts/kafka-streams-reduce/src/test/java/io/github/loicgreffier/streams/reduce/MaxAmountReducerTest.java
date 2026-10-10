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
package io.github.loicgreffier.streams.reduce;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.streams.reduce.app.reducer.MaxAmountReducer;
import java.util.List;
import org.junit.jupiter.api.Test;

class MaxAmountReducerTest {
    @Test
    void shouldKeepHighestAmountOrder() {
        MaxAmountReducer reducer = new MaxAmountReducer();

        Order highest = buildOrder(1L, 1500.0);
        Order middle = buildOrder(2L, 800.0);
        Order lowest = buildOrder(3L, 50.0);

        assertEquals(highest, reducer.apply(lowest, highest));
        assertEquals(highest, reducer.apply(highest, middle));
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
