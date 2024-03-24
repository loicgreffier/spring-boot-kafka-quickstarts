package io.github.loicgreffier.streams.aggregate.hopping.window.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents state store name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class StateStore {
    public static final String PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE =
        "PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE";
}
