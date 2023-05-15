package io.github.loicgreffier.producer.avro.specific.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
