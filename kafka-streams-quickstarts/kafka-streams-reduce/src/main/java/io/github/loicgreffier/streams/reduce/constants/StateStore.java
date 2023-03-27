package io.github.loicgreffier.streams.reduce.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    PERSON_REDUCE_STATE_STORE("PERSON_REDUCE_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}
