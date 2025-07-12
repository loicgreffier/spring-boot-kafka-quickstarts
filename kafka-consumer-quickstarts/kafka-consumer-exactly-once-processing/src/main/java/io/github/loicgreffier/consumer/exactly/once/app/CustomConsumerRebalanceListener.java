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
package io.github.loicgreffier.consumer.exactly.once.app;

import java.util.Collection;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

/**
 * This class represents a custom Kafka consumer rebalance listener that handles partition revocation, assignment, and
 * loss events.
 */
@Slf4j
public class CustomConsumerRebalanceListener implements ConsumerRebalanceListener {
    /**
     * Called when partitions are revoked from the consumer.
     *
     * @param partitions The collection of revoked partitions.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partition revoked: {}", partitions);
    }

    /**
     * Called when partitions are assigned to the consumer.
     *
     * @param partitions The collection of assigned partitions.
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Partition assigned: {}", partitions);
    }

    /**
     * Called when partitions are lost by the consumer.
     *
     * @param partitions The collection of lost partitions.
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        log.info("Partition lost: {}", partitions);

        ConsumerRebalanceListener.super.onPartitionsLost(partitions);
    }
}
