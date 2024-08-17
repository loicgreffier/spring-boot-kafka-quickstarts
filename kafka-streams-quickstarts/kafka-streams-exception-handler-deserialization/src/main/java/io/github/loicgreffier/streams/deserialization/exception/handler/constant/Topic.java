package io.github.loicgreffier.streams.deserialization.exception.handler.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_DESERIALIZATION_EXCEPTION_HANDLER_TOPIC =
        "PERSON_DESERIALIZATION_EXCEPTION_HANDLER_TOPIC";
}
