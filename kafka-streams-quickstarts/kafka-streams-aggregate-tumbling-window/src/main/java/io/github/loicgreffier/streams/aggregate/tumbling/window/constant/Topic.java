package io.github.loicgreffier.streams.aggregate.tumbling.window.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String GROUP_PERSON_BY_LAST_NAME_TOPIC = "GROUP_PERSON_BY_LAST_NAME_TOPIC";
    public static final String PERSON_AGGREGATE_TUMBLING_WINDOW_TOPIC =
        "PERSON_AGGREGATE_TUMBLING_WINDOW_TOPIC";
}
