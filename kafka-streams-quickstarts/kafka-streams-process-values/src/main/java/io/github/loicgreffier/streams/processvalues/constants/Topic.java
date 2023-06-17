package io.github.loicgreffier.streams.processvalues.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_PROCESS_VALUES_TOPIC = "PERSON_PROCESS_VALUES_TOPIC";
}
