package io.github.loicgreffier.streams.deserialization.exception.handler.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_DLQ_TOPIC = "PERSON_DLQ_TOPIC";
}
