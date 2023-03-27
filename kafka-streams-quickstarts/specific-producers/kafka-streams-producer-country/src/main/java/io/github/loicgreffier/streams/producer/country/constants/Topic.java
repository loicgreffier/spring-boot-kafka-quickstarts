package io.github.loicgreffier.streams.producer.country.constants;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public enum Topic {
    COUNTRY_TOPIC("COUNTRY_TOPIC");

    private final String topicName;

    @Override
    public String toString() {
        return topicName;
    }
}
