package io.github.loicgreffier.consumer.simple.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    STRING_TOPIC("STRING_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
