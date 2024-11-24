package io.github.loicgreffier.streams.exception.handler.processing.app.processor;

import io.github.loicgreffier.avro.KafkaPerson;
import java.time.Duration;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

public class ErrorProcessor extends ContextualFixedKeyProcessor<String, KafkaPerson, KafkaPerson> {

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

    @Override
    public void process(FixedKeyRecord<String, KafkaPerson> fixedKeyRecord) {
        if (fixedKeyRecord.value().getFirstName() == null || fixedKeyRecord.value().getLastName() == null) {
            throw new IllegalArgumentException("First name and last name must not be null");
        }
        context().forward(fixedKeyRecord);
    }
}
