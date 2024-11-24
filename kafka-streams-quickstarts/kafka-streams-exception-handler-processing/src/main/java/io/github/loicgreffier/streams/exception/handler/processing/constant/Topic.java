package io.github.loicgreffier.streams.exception.handler.processing.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_PROCESSING_EXCEPTION_HANDLER_TOPIC =
        "PERSON_PROCESSING_EXCEPTION_HANDLER_TOPIC";
}
