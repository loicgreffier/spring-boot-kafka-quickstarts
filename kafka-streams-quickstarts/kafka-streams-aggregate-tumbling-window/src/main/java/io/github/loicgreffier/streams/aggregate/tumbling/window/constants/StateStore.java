package io.github.loicgreffier.streams.aggregate.tumbling.window.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    PERSON_AGGREGATE_TUMBLING_WINDOW_STATE_STORE("PERSON_AGGREGATE_TUMBLING_WINDOW_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}
