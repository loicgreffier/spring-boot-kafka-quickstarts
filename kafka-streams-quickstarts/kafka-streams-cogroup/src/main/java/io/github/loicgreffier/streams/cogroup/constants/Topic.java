package io.github.loicgreffier.streams.cogroup.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_TOPIC_TWO("PERSON_TOPIC_TWO"),
    PERSON_COGROUP_TOPIC("PERSON_COGROUP_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
