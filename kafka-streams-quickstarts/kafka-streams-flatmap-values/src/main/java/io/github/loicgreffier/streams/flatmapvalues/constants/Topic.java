package io.github.loicgreffier.streams.flatmapvalues.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_FLATMAP_VALUES_TOPIC = "PERSON_FLATMAP_VALUES_TOPIC";
}
