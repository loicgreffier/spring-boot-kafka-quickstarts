package io.github.loicgreffier.streams.cogroup.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents state store name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class StateStore {
    public static final String PERSON_COGROUP_AGGREGATE_STATE_STORE =
        "PERSON_COGROUP_AGGREGATE_STATE_STORE";
}
