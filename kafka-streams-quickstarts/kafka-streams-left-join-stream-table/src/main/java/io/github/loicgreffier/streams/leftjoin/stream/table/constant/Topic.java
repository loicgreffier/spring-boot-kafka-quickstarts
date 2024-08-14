package io.github.loicgreffier.streams.leftjoin.stream.table.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String COUNTRY_TOPIC = "COUNTRY_TOPIC";
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC =
        "PERSON_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC";
    public static final String PERSON_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC =
        "PERSON_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC";
}
