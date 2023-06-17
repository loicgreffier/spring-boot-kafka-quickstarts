package io.github.loicgreffier.streams.flatmap.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_FLATMAP_TOPIC = "PERSON_FLATMAP_TOPIC";
}
