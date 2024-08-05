package io.github.loicgreffier.streams.aggregate.hopping.window;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.hopping.window.app.aggregator.FirstNameByLastNameAggregator;
import java.time.Instant;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

/**
 * This class contains unit tests for the {@link FirstNameByLastNameAggregator} class.
 */
class FirstNameByLastNameAggregatorTest {
    @Test
    void shouldAggregateFirstNamesByLastName() {
        FirstNameByLastNameAggregator aggregator = new FirstNameByLastNameAggregator();
        KafkaPersonGroup group = new KafkaPersonGroup(new HashMap<>());

        aggregator.apply("Doe", KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName("John")
            .setLastName("Doe")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build(), group);

        aggregator.apply("Doe", KafkaPerson.newBuilder()
            .setId(2L)
            .setFirstName("Michael")
            .setLastName("Doe")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build(), group);

        aggregator.apply("Smith", KafkaPerson.newBuilder()
            .setId(3L)
            .setFirstName("Jane")
            .setLastName("Smith")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build(), group);

        assertTrue(group.getFirstNameByLastName().containsKey("Doe"));
        assertEquals(2, group.getFirstNameByLastName().get("Doe").size());
        assertEquals("John", group.getFirstNameByLastName().get("Doe").get(0));
        assertEquals("Michael", group.getFirstNameByLastName().get("Doe").get(1));

        assertTrue(group.getFirstNameByLastName().containsKey("Smith"));
        assertEquals(1, group.getFirstNameByLastName().get("Smith").size());
        assertEquals("Jane", group.getFirstNameByLastName().get("Smith").get(0));
    }
}
