package io.github.loicgreffier.streams.leftjoin.stream.globaltable.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String COUNTRY_TOPIC = "COUNTRY_TOPIC";
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_COUNTRY_LEFT_JOIN_STREAM_GLOBAL_TABLE_TOPIC =
        "PERSON_COUNTRY_LEFT_JOIN_STREAM_GLOBAL_TABLE_TOPIC";
}
