package io.lgr.quickstarts.consumer.circuit.breaker.constants;

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
