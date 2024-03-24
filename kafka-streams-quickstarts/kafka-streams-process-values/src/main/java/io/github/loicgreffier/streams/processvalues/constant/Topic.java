package io.github.loicgreffier.streams.processvalues.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_PROCESS_VALUES_TOPIC = "PERSON_PROCESS_VALUES_TOPIC";
}
