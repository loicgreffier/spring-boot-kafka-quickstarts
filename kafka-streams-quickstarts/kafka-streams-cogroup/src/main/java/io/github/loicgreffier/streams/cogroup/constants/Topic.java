package io.github.loicgreffier.streams.cogroup.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_TOPIC_TWO = "PERSON_TOPIC_TWO";
    public static final String PERSON_COGROUP_TOPIC = "PERSON_COGROUP_TOPIC";
    public static final String GROUP_PERSON_BY_LAST_NAME_TOPIC = "GROUP_PERSON_BY_LAST_NAME_TOPIC";
    public static final String GROUP_PERSON_BY_LAST_NAME_TOPIC_TWO =
        "GROUP_PERSON_BY_LAST_NAME_TOPIC_TWO";
}
