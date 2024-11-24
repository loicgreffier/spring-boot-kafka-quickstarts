package io.github.loicgreffier.streams.exception.handler.processing.error;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;

/**
 * Custom processing exception handler.
 */
@Slf4j
public class CustomProcessingExceptionHandler implements ProcessingExceptionHandler {
    @Override
    public ProcessingHandlerResponse handle(ErrorHandlerContext errorHandlerContext,
                                            Record<?, ?> record,
                                            Exception exception) {
        log.warn("Exception caught during processing: key = {}, value = {}",
            record != null ? record.key() :  null,
            record != null ? record.value() : null,
            exception
        );

        return ProcessingHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> map) {
        // Do nothing
    }
}
