package io.lgr.quickstarts.streams.left.join.stream.table.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    COUNTRY_TABLE_LEFT_JOIN_STREAM_MAP_STATE_STORE("COUNTRY_TABLE_LEFT_JOIN_STREAM_MAP_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}
