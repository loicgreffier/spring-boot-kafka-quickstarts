package io.github.loicgreffier.consumer.transaction.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic constants for Kafka topics.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String FIRST_STRING_TOPIC = "FIRST_STRING_TOPIC";
    public static final String SECOND_STRING_TOPIC = "SECOND_STRING_TOPIC";
}
