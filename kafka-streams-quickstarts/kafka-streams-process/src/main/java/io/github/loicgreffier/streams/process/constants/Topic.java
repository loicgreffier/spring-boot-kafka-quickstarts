package io.github.loicgreffier.streams.process.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_PROCESS_TOPIC("PERSON_PROCESS_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
