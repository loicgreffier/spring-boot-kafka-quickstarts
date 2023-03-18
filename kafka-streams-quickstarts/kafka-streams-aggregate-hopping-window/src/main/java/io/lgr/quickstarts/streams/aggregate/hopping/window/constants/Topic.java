package io.lgr.quickstarts.streams.aggregate.hopping.window.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC("PERSON_AGGREGATE_HOPPING_WINDOW_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
