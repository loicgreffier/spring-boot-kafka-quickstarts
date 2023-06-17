package io.github.loicgreffier.streams.aggregate.tumbling.window.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class StateStore {
    public static final String PERSON_AGGREGATE_TUMBLING_WINDOW_STATE_STORE = "PERSON_AGGREGATE_TUMBLING_WINDOW_STATE_STORE";
}
