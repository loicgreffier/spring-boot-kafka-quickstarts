package io.github.loicgreffier.streams.store.window.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents state store name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class StateStore {
    public static final String PERSON_WINDOW_STORE = "PERSON_WINDOW_STORE";
    public static final String PERSON_WINDOW_SUPPLIER_STORE = "PERSON_WINDOW_SUPPLIER_STORE";
}
