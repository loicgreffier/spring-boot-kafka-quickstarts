package io.github.loicgreffier.streams.branch.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_BRANCH_A_TOPIC = "PERSON_BRANCH_A_TOPIC";
    public static final String PERSON_BRANCH_B_TOPIC = "PERSON_BRANCH_B_TOPIC";
    public static final String PERSON_BRANCH_DEFAULT_TOPIC = "PERSON_BRANCH_DEFAULT_TOPIC";
}
