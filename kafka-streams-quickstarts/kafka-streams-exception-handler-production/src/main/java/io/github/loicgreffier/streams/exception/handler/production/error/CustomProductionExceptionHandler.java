package io.github.loicgreffier.streams.exception.handler.production.error;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

/**
 * Custom production exception handler.
 */
@Slf4j
public class CustomProductionExceptionHandler implements ProductionExceptionHandler {

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        if (exception instanceof RecordTooLargeException) {
            log.warn("Record too large exception caught during production: topic = {}, partition = {}",
                record.topic(), record.partition(), exception);

            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        log.warn("Exception caught during production: topic = {}, partition = {}",
            record.topic(), record.partition(), exception);

        return ProductionExceptionHandlerResponse.FAIL;
    }

    @Override
    public ProductionExceptionHandlerResponse handleSerializationException(ProducerRecord record, Exception exception) {
        log.warn("Exception caught during serialization: topic = {}, partition = {}",
            record.topic(), record.partition(), exception);

        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Do nothing
    }
}
