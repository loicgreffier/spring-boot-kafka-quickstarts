package io.github.loicgreffier.streams.aggregate.tumbling.window;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.tumbling.window.app.aggregator.FirstNameByLastNameAggregator;
import java.time.Instant;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

class FirstNameByLastNameAggregatorTest {
    @Test
    void shouldAggregateFirstNamesByLastName() {
        FirstNameByLastNameAggregator aggregator = new FirstNameByLastNameAggregator();
        KafkaPersonGroup group = new KafkaPersonGroup(new HashMap<>());

        aggregator.apply("Simpson", KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName("Homer")
            .setLastName("Simpson")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build(), group);

        aggregator.apply("Simpson", KafkaPerson.newBuilder()
            .setId(2L)
            .setFirstName("Marge")
            .setLastName("Simpson")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build(), group);

        aggregator.apply("Van Houten", KafkaPerson.newBuilder()
            .setId(3L)
            .setFirstName("Milhouse")
            .setLastName("Van Houten")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00Z"))
            .build(), group);

        assertTrue(group.getFirstNameByLastName().containsKey("Simpson"));
        assertEquals(2, group.getFirstNameByLastName().get("Simpson").size());
        assertEquals("Homer", group.getFirstNameByLastName().get("Simpson").get(0));
        assertEquals("Marge", group.getFirstNameByLastName().get("Simpson").get(1));

        assertTrue(group.getFirstNameByLastName().containsKey("Van Houten"));
        assertEquals(1, group.getFirstNameByLastName().get("Van Houten").size());
        assertEquals("Milhouse", group.getFirstNameByLastName().get("Van Houten").get(0));
    }
}
