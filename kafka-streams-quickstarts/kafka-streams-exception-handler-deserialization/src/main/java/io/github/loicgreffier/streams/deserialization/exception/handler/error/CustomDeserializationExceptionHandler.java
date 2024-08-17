package io.github.loicgreffier.streams.deserialization.exception.handler.error;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * Custom deserialization exception handler.
 */
@Slf4j
public class CustomDeserializationExceptionHandler implements DeserializationExceptionHandler {

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {
        log.warn("Exception caught during deserialization: taskId = {}, topic = {}, partition = {}, offset = {}",
            context.taskId(), record.topic(), record.partition(), record.offset(), exception);
        
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Do nothing
    }
}
