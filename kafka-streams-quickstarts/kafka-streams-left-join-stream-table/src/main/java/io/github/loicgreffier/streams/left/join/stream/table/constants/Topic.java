package io.github.loicgreffier.streams.left.join.stream.table.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    COUNTRY_TOPIC("COUNTRY_TOPIC"),
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC("PERSON_LEFT_JOIN_STREAM_TABLE_REKEY_TOPIC"),
    PERSON_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC("PERSON_COUNTRY_LEFT_JOIN_STREAM_TABLE_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}