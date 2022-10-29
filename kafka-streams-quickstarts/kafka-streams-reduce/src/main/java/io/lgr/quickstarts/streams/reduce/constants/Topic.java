package io.lgr.quickstarts.streams.reduce.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_REDUCE_TOPIC("PERSON_REDUCE_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
