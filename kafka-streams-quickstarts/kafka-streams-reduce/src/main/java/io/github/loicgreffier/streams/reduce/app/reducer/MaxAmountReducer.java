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
package io.github.loicgreffier.streams.reduce.app.reducer;

import io.github.loicgreffier.avro.Order;
import org.apache.kafka.streams.kstream.Reducer;

/** This class represents a reducer that reduces the input values to the one with the highest amount. */
public class MaxAmountReducer implements Reducer<Order> {
    /**
     * Reduces the input values to the one with the highest amount.
     *
     * @param reduced The reduced value.
     * @param toReduce The value to reduce.
     * @return The reduced value.
     */
    @Override
    public Order apply(Order reduced, Order toReduce) {
        return toReduce.getAmount() > reduced.getAmount() ? toReduce : reduced;
    }
}
