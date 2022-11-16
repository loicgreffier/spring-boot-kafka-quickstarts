package io.lgr.quickstarts.streams.left.join.stream.stream.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE("PERSON_LEFT_JOIN_STREAM_STREAM_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}
