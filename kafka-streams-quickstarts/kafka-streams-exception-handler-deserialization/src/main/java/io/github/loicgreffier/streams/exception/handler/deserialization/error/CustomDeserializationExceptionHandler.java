package io.github.loicgreffier.streams.exception.handler.deserialization.error;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

/**
 * Custom deserialization exception handler.
 */
@Slf4j
public class CustomDeserializationExceptionHandler implements DeserializationExceptionHandler {

    @Override
    public DeserializationHandlerResponse handle(ErrorHandlerContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {
        log.warn("Exception caught during deserialization: taskId = {}, topic = {}, partition = {}, offset = {}",
            context.taskId(),
            context.topic(),
            context.partition(),
            context.offset(),
            exception);
        
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Do nothing
    }
}
