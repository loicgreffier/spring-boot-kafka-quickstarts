package io.github.loicgreffier.streams.average.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_AVERAGE_TOPIC = "PERSON_AVERAGE_TOPIC";
    public static final String GROUP_PERSON_BY_NATIONALITY_TOPIC = "GROUP_PERSON_BY_NATIONALITY_TOPIC";
}
