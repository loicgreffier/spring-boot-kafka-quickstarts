package io.github.loicgreffier.streams.join.stream.stream.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    PERSON_JOIN_STREAM_STREAM_STATE_STORE("PERSON_JOIN_STREAM_STREAM_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}
