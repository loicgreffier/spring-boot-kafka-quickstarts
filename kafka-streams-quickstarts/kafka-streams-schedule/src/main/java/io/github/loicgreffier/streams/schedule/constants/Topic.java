package io.github.loicgreffier.streams.schedule.constants;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Topic {
    public static final String PERSON_TOPIC = "PERSON_TOPIC";
    public static final String PERSON_SCHEDULE_TOPIC = "PERSON_SCHEDULE_TOPIC";
}