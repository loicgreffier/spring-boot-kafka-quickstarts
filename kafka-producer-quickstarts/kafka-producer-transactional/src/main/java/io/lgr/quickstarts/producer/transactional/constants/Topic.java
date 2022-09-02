package io.lgr.quickstarts.producer.transactional.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    FIRST_STRING_TOPIC("FIRST_STRING_TOPIC"),
    SECOND_STRING_TOPIC("SECOND_STRING_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
