package io.github.loicgreffier.streams.reduce.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents state store name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class StateStore {
    public static final String PERSON_REDUCE_STATE_STORE = "PERSON_REDUCE_STATE_STORE";
}
