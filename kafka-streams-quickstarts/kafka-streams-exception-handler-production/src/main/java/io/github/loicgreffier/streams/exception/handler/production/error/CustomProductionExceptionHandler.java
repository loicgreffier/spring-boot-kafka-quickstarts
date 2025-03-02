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

package io.github.loicgreffier.streams.exception.handler.production.error;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

/**
 * Custom production exception handler.
 */
@Slf4j
public class CustomProductionExceptionHandler implements ProductionExceptionHandler {

    @Override
    public ProductionExceptionHandlerResponse handle(ErrorHandlerContext context,
                                                     ProducerRecord<byte[], byte[]> record,
                                                     Exception exception) {
        if (exception instanceof RecordTooLargeException) {
            log.warn("Record too large exception caught for "
                    + "processorNodeId = {}, topic = {}, partition = {}, offset = {}",
                context.processorNodeId(),
                context.topic(),
                context.partition(),
                context.offset(),
                exception
            );

            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        log.warn("Exception caught during production for processorNodeId = {}, topic = {}, partition = {}, offset = {}",
            context.processorNodeId(),
            context.topic(),
            context.partition(),
            context.offset(),
            exception
        );

        return ProductionExceptionHandlerResponse.FAIL;
    }

    @Override
    public ProductionExceptionHandlerResponse handleSerializationException(ErrorHandlerContext context,
                                                                           ProducerRecord record,
                                                                           Exception exception,
                                                                           SerializationExceptionOrigin origin) {
        log.warn("Exception caught during serialization for "
                + "topic = {}, partition = {}, offset = {}, origin = {}, key = {}, value = {}",
            context.topic(),
            context.partition(),
            context.offset(),
            origin,
            record.key(),
            record.value(),
            exception
        );

        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Do nothing
    }
}
