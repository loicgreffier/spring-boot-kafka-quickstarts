package io.github.loicgreffier.streams.producer.person.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_TOPIC_TWO = "PERSON_TOPIC_TWO";
}
