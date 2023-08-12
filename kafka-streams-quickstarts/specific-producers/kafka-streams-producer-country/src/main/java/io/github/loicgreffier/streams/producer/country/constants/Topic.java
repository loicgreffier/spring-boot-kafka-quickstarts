package io.github.loicgreffier.streams.producer.country.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic constants for Kafka topics.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String COUNTRY_TOPIC = "COUNTRY_TOPIC";
}
