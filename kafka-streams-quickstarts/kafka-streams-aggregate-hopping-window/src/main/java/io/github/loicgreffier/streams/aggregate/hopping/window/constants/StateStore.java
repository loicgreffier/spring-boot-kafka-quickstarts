package io.github.loicgreffier.streams.aggregate.hopping.window.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE("PERSON_AGGREGATE_HOPPING_WINDOW_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}
