package io.github.loicgreffier.streams.selectkey.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_SELECT_KEY_TOPIC("PERSON_SELECT_KEY_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
