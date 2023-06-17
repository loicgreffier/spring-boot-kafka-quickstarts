package io.github.loicgreffier.streams.selectkey.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_SELECT_KEY_TOPIC = "PERSON_SELECT_KEY_TOPIC";
}
