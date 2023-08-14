package io.github.loicgreffier.streams.branch.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_BRANCH_A_TOPIC = "PERSON_BRANCH_A_TOPIC";
    public static final String PERSON_BRANCH_B_TOPIC = "PERSON_BRANCH_B_TOPIC";
    public static final String PERSON_BRANCH_DEFAULT_TOPIC = "PERSON_BRANCH_DEFAULT_TOPIC";
}
