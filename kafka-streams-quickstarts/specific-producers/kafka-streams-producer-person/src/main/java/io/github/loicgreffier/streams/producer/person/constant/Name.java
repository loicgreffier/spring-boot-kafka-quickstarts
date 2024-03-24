package io.github.loicgreffier.streams.producer.person.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * This abstract class represents name constants for Kafka records.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public abstract class Name {
    public static final String[] FIRST_NAMES =
        new String[] {"Liam", "Noah", "Ethan", "Oliver", "Aiden", "Caden", "Elijah", "Logan",
            "Mason", "Lucas", "James", "Benjamin", "Henry", "Alexander", "Sebastian", "Michael",
            "Daniel", "William", "David", "Joseph", "Ella", "Sophia", "Isabella", "Mia",
            "Charlotte", "Amelia", "Harper", "Evelyn", "Abigail", "Emily", "Elizabeth", "Sofia",
            "Avery", "Ella", "Scarlett", "Grace", "Chloe", "Victoria", "Riley", "Aria", "Lily",
            "Aubrey", "Zoey", "Penelope", "Luna", "Hazel", "Nora", "Leah", "Skyler", "Camila"};

    public static final String[] LAST_NAMES =
        new String[] {"Smith", "Johnson", "Williams", "Brown", "Jones", "Miller", "Davis", "Garcia",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
            "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
            "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
            "Doe", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
            "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter",
            "Roberts"};
}
