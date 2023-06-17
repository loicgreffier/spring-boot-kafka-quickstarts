package io.github.loicgreffier.streams.reduce.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_REDUCE_TOPIC = "PERSON_REDUCE_TOPIC";
    public static final String GROUP_PERSON_BY_NATIONALITY_TOPIC = "GROUP_PERSON_BY_NATIONALITY_TOPIC";
}
