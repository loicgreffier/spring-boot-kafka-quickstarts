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

import static io.github.loicgreffier.streams.reconciliation.constant.StateStore.RECONCILIATION_STORE;

import io.github.loicgreffier.avro.KafkaOrder;
import io.github.loicgreffier.avro.KafkaReconciliation;
import io.github.loicgreffier.avro.KafkaUser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * This class is a processor that handles the reconciliation.
 *
 * @param <T> The type of the value in the record being processed.
 */
@Slf4j
public class ReconciliationProcessor<T> extends ContextualProcessor<String, T, String, KafkaReconciliation> {
    private KeyValueStore<String, KafkaReconciliation> reconciliationStore;

    /**
     * Initialize the processor.
     *
     * @param context The processor context.
     */
    @Override
    public void init(ProcessorContext<String, KafkaReconciliation> context) {
        super.init(context);
        reconciliationStore = context.getStateStore(RECONCILIATION_STORE);
    }

    /**
     * Process a record and perform reconciliation. Checks whether a reconciliation record exists for the given key. If
     * it does not exist, a new reconciliation record is created. If the record is a {@code KafkaUser}, the customer is
     * set in the reconciliation record. If the record is a {@code KafkaOrder}, the order is set in the reconciliation
     * record. If both customer and order are present in the reconciliation record, the record is emitted and removed
     * from the store. Otherwise, the current state of the reconciliation record is logged.
     *
     * @param message The message to process.
     */
    @Override
    public void process(Record<String, T> message) {
        log.info("Received record {}", message.value().getClass().getSimpleName());

        String customerId = message.key();
        KafkaReconciliation reconciliation = reconciliationStore.get(customerId);

        if (reconciliation == null) {
            log.info("No reconciliation record found for key = {}. Storing record in the store", customerId);
            reconciliation = new KafkaReconciliation();
        }

        if (message.value() instanceof KafkaUser kafkaUser) {
            reconciliation.setCustomer(kafkaUser);
        } else if (message.value() instanceof KafkaOrder kafkaOrder) {
            reconciliation.setOrder(kafkaOrder);
        }

        reconciliationStore.put(customerId, reconciliation);

        log.info(
                "Reconciliation record for key = {} updated in the store. Checking if reconciliation is complete",
                customerId);

        if (reconciliation.getCustomer() != null && reconciliation.getOrder() != null) {
            log.info("Reconciliation record for key = {} is complete. Emitting record", customerId);
            reconciliationStore.delete(customerId);
            context().forward(new Record<>(customerId, reconciliation, context().currentSystemTimeMs()));
        } else {
            log.info(
                    "Reconciliation record for key = {} is not complete yet. Has customer = {}, has order = {}",
                    customerId,
                    reconciliation.getCustomer() != null,
                    reconciliation.getOrder() != null);
        }
    }
}
