package io.lgr.quickstarts.streams.count.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    PERSON_COUNT_STATE_STORE("PERSON_COUNT_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}
