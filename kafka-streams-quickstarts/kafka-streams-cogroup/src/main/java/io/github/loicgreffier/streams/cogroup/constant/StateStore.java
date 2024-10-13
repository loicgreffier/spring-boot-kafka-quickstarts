package io.github.loicgreffier.streams.cogroup.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents state store name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class StateStore {
    public static final String PERSON_COGROUP_AGGREGATE_STORE = "PERSON_COGROUP_AGGREGATE_STORE";
}
