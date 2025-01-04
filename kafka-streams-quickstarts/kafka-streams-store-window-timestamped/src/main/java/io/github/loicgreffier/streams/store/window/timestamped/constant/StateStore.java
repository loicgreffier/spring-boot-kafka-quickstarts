package io.github.loicgreffier.streams.store.window.timestamped.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents state store name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class StateStore {
    public static final String PERSON_TIMESTAMPED_WINDOW_STORE = "PERSON_TIMESTAMPED_WINDOW_STORE";
    public static final String PERSON_TIMESTAMPED_WINDOW_SUPPLIER_STORE = "PERSON_TIMESTAMPED_WINDOW_SUPPLIER_STORE";
}
