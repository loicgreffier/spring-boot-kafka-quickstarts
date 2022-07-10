package io.lgr.quickstarts.streams.map.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_UPPERCASE_TOPIC("PERSON_UPPERCASE_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
