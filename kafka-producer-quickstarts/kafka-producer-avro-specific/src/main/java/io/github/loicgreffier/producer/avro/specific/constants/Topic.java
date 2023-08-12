package io.github.loicgreffier.producer.avro.specific.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic constants for Kafka topics.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
}
