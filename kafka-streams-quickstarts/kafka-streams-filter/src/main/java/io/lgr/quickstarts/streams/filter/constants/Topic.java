package io.lgr.quickstarts.streams.filter.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_FILTER_TOPIC("PERSON_FILTER_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
