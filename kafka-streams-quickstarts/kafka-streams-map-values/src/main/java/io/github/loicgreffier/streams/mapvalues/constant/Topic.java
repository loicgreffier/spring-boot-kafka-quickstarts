package io.github.loicgreffier.streams.mapvalues.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_MAP_VALUES_TOPIC = "PERSON_MAP_VALUES_TOPIC";
}
