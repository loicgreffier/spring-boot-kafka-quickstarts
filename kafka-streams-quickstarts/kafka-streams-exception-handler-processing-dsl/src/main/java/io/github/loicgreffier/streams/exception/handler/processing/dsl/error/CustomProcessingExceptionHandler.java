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
package io.github.loicgreffier.streams.exception.handler.processing.dsl.error;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;

/** Custom processing exception handler. */
@Slf4j
public class CustomProcessingExceptionHandler implements ProcessingExceptionHandler {
    @Override
    public ProcessingHandlerResponse handle(ErrorHandlerContext context, Record<?, ?> message, Exception exception) {
        log.warn(
                "Exception caught for processorNodeId = {}, topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                context.processorNodeId(),
                context.topic(),
                context.partition(),
                context.offset(),
                message != null ? message.key() : null,
                message != null ? message.value() : null,
                exception);

        if (exception instanceof IllegalArgumentException) {
            return ProcessingHandlerResponse.CONTINUE;
        }

        return ProcessingHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> map) {
        // Do nothing
    }
}
