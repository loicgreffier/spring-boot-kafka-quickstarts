package io.github.loicgreffier.producer.simple.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic constants for Kafka topics.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String STRING_TOPIC = "STRING_TOPIC";
}