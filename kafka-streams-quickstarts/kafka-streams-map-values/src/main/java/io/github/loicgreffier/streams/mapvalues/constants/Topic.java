package io.github.loicgreffier.streams.mapvalues.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_MAP_VALUES_TOPIC("PERSON_MAP_VALUES_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
