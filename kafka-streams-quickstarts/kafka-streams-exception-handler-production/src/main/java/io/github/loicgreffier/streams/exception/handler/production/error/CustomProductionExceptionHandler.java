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
            log.warn("Record too large exception caught during production: "
                    + "processorNodeId = {}, topic = {}, partition = {}, offset = {}",
                context.processorNodeId(),
                context.topic(),
                context.partition(),
                context.offset(),
                exception);

            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        log.warn("Exception caught during production: " +
                "processorNodeId = {}, topic = {}, partition = {}, offset = {}",
            context.processorNodeId(),
            context.topic(),
            context.partition(),
            context.offset(),
            exception);

        return ProductionExceptionHandlerResponse.FAIL;
    }

    @Override
    public ProductionExceptionHandlerResponse handleSerializationException(ErrorHandlerContext context,
                                                                           ProducerRecord record,
                                                                           Exception exception,
                                                                           SerializationExceptionOrigin origin) {
        log.warn("Exception caught during serialization: topic = {}, partition = {}, offset = {}, origin = {}",
            context.topic(),
            context.partition(),
            context.offset(),
            origin,
            exception);

        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Do nothing
    }
}
