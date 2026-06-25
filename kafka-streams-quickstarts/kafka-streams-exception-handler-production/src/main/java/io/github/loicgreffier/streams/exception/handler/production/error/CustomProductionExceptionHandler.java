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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Custom production exception handler. */
public class CustomProductionExceptionHandler implements ProductionExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(CustomProductionExceptionHandler.class);

    /**
     * Handles a production exception.
     *
     * @param context The error handler context.
     * @param message The record that failed to be produced.
     * @param exception The exception that occurred.
     * @return The response indicating whether to continue or fail processing.
     */
    @Override
    public Response handleError(
            ErrorHandlerContext context, ProducerRecord<byte[], byte[]> message, Exception exception) {
        if (exception instanceof RecordTooLargeException) {
            log.atWarn()
                    .addArgument(context.processorNodeId())
                    .addArgument(context.topic())
                    .addArgument(context.partition())
                    .addArgument(context.offset())
                    .setCause(exception)
                    .log(
                            "Record too large exception caught for processorNodeId = {}, topic = {}, partition = {}, offset = {}");

            return Response.resume();
        }

        log.atWarn()
                .addArgument(context.processorNodeId())
                .addArgument(context.topic())
                .addArgument(context.partition())
                .addArgument(context.offset())
                .setCause(exception)
                .log(
                        "Exception caught during production for processorNodeId = {}, topic = {}, partition = {}, offset = {}");

        return Response.fail();
    }

    /**
     * Handles a serialization exception by resuming processing.
     *
     * @param context The error handler context.
     * @param message The record that failed to be serialized.
     * @param exception The exception that occurred.
     * @param origin The origin of the serialization exception.
     * @return The response indicating whether to continue or fail processing.
     */
    @Override
    public Response handleSerializationError(
            ErrorHandlerContext context,
            ProducerRecord message,
            Exception exception,
            SerializationExceptionOrigin origin) {
        log.atWarn()
                .addArgument(context.topic())
                .addArgument(context.partition())
                .addArgument(context.offset())
                .addArgument(origin)
                .addArgument(message.key())
                .addArgument(message.value())
                .setCause(exception)
                .log("Exception caught during serialization for topic = {}, partition = {},"
                        + " offset = {}, origin = {}, key = {}, value = {}");

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
