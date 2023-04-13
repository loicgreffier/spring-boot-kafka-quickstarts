package io.github.loicgreffier.streams.processvalues.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    PERSON_PROCESS_VALUES_STATE_STORE("PERSON_PROCESS_VALUES_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}
