package io.github.loicgreffier.streams.leftjoin.stream.stream.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Topic name constants.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_TOPIC_TWO = "PERSON_TOPIC_TWO";
    public static final String PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC =
        "PERSON_LEFT_JOIN_STREAM_STREAM_REKEY_TOPIC";
    public static final String PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC =
        "PERSON_LEFT_JOIN_STREAM_STREAM_TOPIC";
}
