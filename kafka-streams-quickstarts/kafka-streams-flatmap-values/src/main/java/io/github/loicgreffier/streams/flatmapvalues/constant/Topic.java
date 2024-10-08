package io.github.loicgreffier.streams.flatmapvalues.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_FLATMAP_VALUES_TOPIC = "PERSON_FLATMAP_VALUES_TOPIC";
}
