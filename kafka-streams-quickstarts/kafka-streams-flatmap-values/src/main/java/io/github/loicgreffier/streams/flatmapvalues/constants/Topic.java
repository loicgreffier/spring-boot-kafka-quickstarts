package io.github.loicgreffier.streams.flatmapvalues.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_FLATMAP_VALUES_TOPIC("PERSON_FLATMAP_VALUES_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
