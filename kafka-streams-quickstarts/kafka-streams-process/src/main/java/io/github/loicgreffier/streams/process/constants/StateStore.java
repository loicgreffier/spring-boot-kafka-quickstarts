package io.github.loicgreffier.streams.process.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    PERSON_PROCESS_STATE_STORE("PERSON_PROCESS_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}
