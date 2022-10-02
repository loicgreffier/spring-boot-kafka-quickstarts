package io.lgr.quickstarts.streams.cogroup.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    PERSON_COGROUP_AGGREGATE_STATE_STORE("PERSON_COGROUP_AGGREGATE_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}
