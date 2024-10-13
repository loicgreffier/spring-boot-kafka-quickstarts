package io.github.loicgreffier.streams.store.keyvalue.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents state store name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class StateStore {
    public static final String PERSON_KEY_VALUE_STORE = "PERSON_KEY_VALUE_STORE";
    public static final String PERSON_KEY_VALUE_SUPPLIER_STORE = "PERSON_KEY_VALUE_SUPPLIER_STORE";
}
