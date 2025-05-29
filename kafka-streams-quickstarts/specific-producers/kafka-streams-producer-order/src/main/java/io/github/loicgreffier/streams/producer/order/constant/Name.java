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
package io.github.loicgreffier.streams.producer.order.constant;

import lombok.AllArgsConstructor;
import lombok.Getter;

/** This abstract class represents name constants for Kafka records. */
public abstract class Name {
    public static final Item[] ITEMS = new Item[] {
        new Item("Milk", 2.99),
        new Item("Bread", 1.49),
        new Item("Eggs", 3.19),
        new Item("Butter", 2.59),
        new Item("Cheese", 4.89),
        new Item("Apples", 0.99),
        new Item("Bananas", 0.59),
        new Item("Oranges", 1.29),
        new Item("Grapes", 2.79),
        new Item("Strawberries", 3.99),
        new Item("Chicken Breast", 5.49),
        new Item("Ground Beef", 6.79),
        new Item("Pork Chops", 5.29),
        new Item("Salmon", 9.99),
        new Item("Bacon", 4.49),
        new Item("Potatoes", 1.99),
        new Item("Carrots", 1.09),
        new Item("Broccoli", 1.79),
        new Item("Onions", 0.89),
        new Item("Tomatoes", 1.49),
        new Item("Lettuce", 1.19),
        new Item("Spinach", 2.29),
        new Item("Cucumber", 0.99),
        new Item("Bell Peppers", 2.49),
        new Item("Garlic", 0.59),
        new Item("Rice", 3.49),
        new Item("Pasta", 1.29),
        new Item("Flour", 2.19),
        new Item("Sugar", 2.39),
        new Item("Salt", 0.89),
        new Item("Black Pepper", 2.99),
        new Item("Olive Oil", 6.49),
        new Item("Canola Oil", 3.99),
        new Item("Cereal", 4.59),
        new Item("Oatmeal", 3.29),
        new Item("Peanut Butter", 3.89),
        new Item("Jam", 2.79),
        new Item("Honey", 4.99),
        new Item("Yogurt", 1.49),
        new Item("Ice Cream", 5.29),
        new Item("Frozen Pizza", 4.99),
        new Item("Frozen Vegetables", 2.59),
        new Item("Canned Beans", 1.09),
        new Item("Canned Tuna", 1.39),
        new Item("Soup", 1.69),
        new Item("Soda", 1.19),
        new Item("Juice", 2.79),
        new Item("Coffee", 7.49),
        new Item("Tea", 3.19),
        new Item("Bottled Water", 0.99)
    };

    @Getter
    @AllArgsConstructor
    public static class Item {
        private String name;
        private double price;
    }

    /** Private constructor. */
    private Name() {}
}
