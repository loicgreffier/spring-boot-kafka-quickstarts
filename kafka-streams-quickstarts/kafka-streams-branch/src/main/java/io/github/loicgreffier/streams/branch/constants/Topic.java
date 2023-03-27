package io.github.loicgreffier.streams.branch.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    PERSON_TOPIC("PERSON_TOPIC"),
    PERSON_BRANCH_A_TOPIC("PERSON_BRANCH_A_TOPIC"),
    PERSON_BRANCH_B_TOPIC("PERSON_BRANCH_B_TOPIC"),
    PERSON_BRANCH_DEFAULT_TOPIC("PERSON_BRANCH_DEFAULT_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
