package io.github.loicgreffier.streams.average.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_AVERAGE_TOPIC = "PERSON_AVERAGE_TOPIC";
    public static final String GROUP_PERSON_BY_NATIONALITY_TOPIC =
        "GROUP_PERSON_BY_NATIONALITY_TOPIC";
}
