package io.lgr.quickstarts.streams.aggregate.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    PERSON_AGGREGATE_STATE_STORE("PERSON_AGGREGATE_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}
