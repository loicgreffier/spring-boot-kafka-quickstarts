package io.github.loicgreffier.streams.count.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class StateStore {
    public static final String PERSON_COUNT_STATE_STORE = "PERSON_COUNT_STATE_STORE";
}
