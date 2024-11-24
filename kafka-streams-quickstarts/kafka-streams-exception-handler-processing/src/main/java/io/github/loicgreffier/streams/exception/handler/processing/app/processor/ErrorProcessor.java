package io.github.loicgreffier.streams.exception.handler.processing.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import java.time.Duration;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

/**
 * This class represents a processor that throws an exception during processing and punctuation.
 */
public class ErrorProcessor extends ContextualFixedKeyProcessor<String, KafkaPerson, KafkaPerson> {

    /**
     * Initialize the processor.
     *
     * @param context the processor context.
     */
    @Override
    public void init(FixedKeyProcessorContext<String, KafkaPerson> context) {
        super.init(context);
        context.schedule(Duration.ofMinutes(1),
            PunctuationType.WALL_CLOCK_TIME,
            timestamp -> {
                throw new RuntimeException("Forced processing exception during punctuation");
            }
        );
    }

    /**
     * Process the record by throwing an exception if the first name or last name is null.
     *
     * @param message the record to process
     */
    @Override
    public void process(FixedKeyRecord<String, KafkaPerson> message) {
        if (message.value().getFirstName() == null || message.value().getLastName() == null) {
            throw new IllegalArgumentException("First name and last name must not be null");
        }
        context().forward(message);
    }
}
