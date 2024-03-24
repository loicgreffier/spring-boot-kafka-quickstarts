package io.github.loicgreffier.streams.schedule.store.cleanup.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents state store name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class StateStore {
    public static final String PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE =
        "PERSON_SCHEDULE_STORE_CLEANUP_STATE_STORE";
}
