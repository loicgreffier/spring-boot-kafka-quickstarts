package io.github.loicgreffier.streams.join.stream.global.table.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum StateStore {
    COUNTRY_GLOBAL_TABLE_JOIN_STREAM_GLOBAL_TABLE_STATE_STORE("COUNTRY_GLOBAL_TABLE_JOIN_STREAM_GLOBAL_TABLE_STATE_STORE");

    private final String stateStoreName;

    @Override
    public String toString() {
        return stateStoreName;
    }
}