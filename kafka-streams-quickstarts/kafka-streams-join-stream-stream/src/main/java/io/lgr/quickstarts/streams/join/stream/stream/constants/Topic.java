package io.lgr.quickstarts.streams.join.stream.stream.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_TOPIC_TWO("PERSON_TOPIC_TWO"),
    PERSON_JOIN_STREAM_STREAM_REKEY_TOPIC("PERSON_JOIN_STREAM_STREAM_REKEY_TOPIC"),

    PERSON_JOIN_STREAM_STREAM_TOPIC("PERSON_JOIN_STREAM_STREAM_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
