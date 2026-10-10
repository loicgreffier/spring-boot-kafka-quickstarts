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
package io.github.loicgreffier.producer.transaction.model;

/**
 * This record represents a payment of an order.
 *
 * @param id The payment id.
 * @param orderId The paid order id.
 * @param amount The payment amount.
 */
public record Payment(long id, long orderId, double amount) {
    /**
     * Converts the payment to a JSON string.
     *
     * @return The JSON string.
     */
    public String toJson() {
        return "{\"id\":%d,\"orderId\":%d,\"amount\":%s}".formatted(id, orderId, amount);
    }
}
