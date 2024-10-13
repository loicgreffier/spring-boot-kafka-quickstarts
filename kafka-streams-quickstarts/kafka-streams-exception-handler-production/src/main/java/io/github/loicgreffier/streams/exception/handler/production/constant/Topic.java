package io.github.loicgreffier.streams.exception.handler.production.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_PRODUCTION_EXCEPTION_HANDLER_TOPIC = "PERSON_PRODUCTION_EXCEPTION_HANDLER_TOPIC";
}
