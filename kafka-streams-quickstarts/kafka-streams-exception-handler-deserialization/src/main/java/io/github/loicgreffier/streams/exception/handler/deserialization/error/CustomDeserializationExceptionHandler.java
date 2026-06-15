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
package io.github.loicgreffier.streams.exception.handler.deserialization.error;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Custom deserialization exception handler. */
public class CustomDeserializationExceptionHandler implements DeserializationExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(CustomDeserializationExceptionHandler.class);

    /**
     * Handles a deserialization exception by resuming processing.
     *
     * @param context The error handler context.
     * @param message The record that failed to be deserialized.
     * @param exception The exception that occurred.
     * @return The response indicating whether to continue or fail processing.
     */
    @Override
    public Response handleError(
            ErrorHandlerContext context, ConsumerRecord<byte[], byte[]> message, Exception exception) {
        log.warn(
                "Exception caught for processorNodeId = {}, topic = {}, partition = {}, offset = {}",
                context.processorNodeId(),
                context.topic(),
                context.partition(),
                context.offset(),
                exception);

        return Response.resume();
    }

    /**
     * Configures the handler.
     *
     * @param configs The configuration properties.
     */
    @Override
    public void configure(Map<String, ?> configs) {
        // Do nothing
    }
}
