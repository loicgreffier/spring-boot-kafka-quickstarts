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
package io.github.loicgreffier.streams.reconciliation.app.processor;

import static io.github.loicgreffier.streams.reconciliation.constant.StateStore.RECONCILIATION_STORE;

import io.github.loicgreffier.avro.Order;
import io.github.loicgreffier.avro.Payment;
import io.github.loicgreffier.avro.Reconciliation;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a processor that handles the reconciliation.
 *
 * @param <T> The type of the value in the record being processed.
 */
public class ReconciliationProcessor<T> extends ContextualProcessor<String, T, String, Reconciliation> {
    private static final Logger log = LoggerFactory.getLogger(ReconciliationProcessor.class);
    private KeyValueStore<String, Reconciliation> reconciliationStore;

    /**
     * Initialize the processor.
     *
     * @param context The processor context.
     */
    @Override
    public void init(ProcessorContext<String, Reconciliation> context) {
        super.init(context);
        reconciliationStore = context.getStateStore(RECONCILIATION_STORE);
    }

    /**
     * Process a record and perform reconciliation. Checks whether a reconciliation record exists for the given order
     * id. If it does not exist, a new reconciliation record is created. If the record is an {@code Order}, the order is
     * set in the reconciliation record. If the record is a {@code Payment}, the payment is set in the reconciliation
     * record. If both order and payment are present in the reconciliation record, the record is emitted and removed
     * from the store. Otherwise, the current state of the reconciliation record is logged.
     *
     * @param message The message to process.
     */
    @Override
    public void process(Record<String, T> message) {
        log.info("Processing record {}", message.value().getClass().getSimpleName());

        String orderId = message.key();
        Reconciliation reconciliation = reconciliationStore.get(orderId);

        if (reconciliation == null) {
            log.info("No reconciliation record found for key = {}. Storing record in the store", orderId);
            reconciliation = new Reconciliation();
        }

        if (message.value() instanceof Order order) {
            reconciliation.setOrder(order);
        }

        if (message.value() instanceof Payment payment) {
            reconciliation.setPayment(payment);
        }

        reconciliationStore.put(orderId, reconciliation);
        log.info(
                "Reconciliation record for key = {} updated in the store. Checking if reconciliation is complete",
                orderId);

        if (reconciliation.getOrder() == null || reconciliation.getPayment() == null) {
            log.info(
                    "Reconciliation record for key = {} is not complete yet. Has order = {}, has payment = {}",
                    orderId,
                    reconciliation.getOrder() != null,
                    reconciliation.getPayment() != null);
            return;
        }

        log.info("Reconciliation record for key = {} is complete. Emitting record", orderId);
        reconciliationStore.delete(orderId);
        context().forward(new Record<>(orderId, reconciliation, context().currentSystemTimeMs()));
    }
}
