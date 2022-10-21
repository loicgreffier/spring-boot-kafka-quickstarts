package io.lgr.quickstarts.streams.join.stream.table.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    COUNTRY_TOPIC("COUNTRY_TOPIC"),
    COUNTRY_REKEY_TOPIC("COUNTRY_REKEY_TOPIC"),
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_REKEY_TOPIC("PERSON_REKEY_TOPIC"),
    JOIN_PERSON_COUNTRY_TOPIC("JOIN_PERSON_COUNTRY_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
