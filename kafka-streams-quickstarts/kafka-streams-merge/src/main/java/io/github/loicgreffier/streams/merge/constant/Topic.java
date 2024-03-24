package io.github.loicgreffier.streams.merge.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_TOPIC_TWO = "PERSON_TOPIC_TWO";
    public static final String PERSON_MERGE_TOPIC = "PERSON_MERGE_TOPIC";
}
