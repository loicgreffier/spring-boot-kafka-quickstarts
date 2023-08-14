package io.github.loicgreffier.streams.aggregate;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.avro.KafkaPersonGroup;
import io.github.loicgreffier.streams.aggregate.app.aggregator.FirstNameByLastNameAggregator;
import java.time.Instant;
import java.util.HashMap;
import org.junit.jupiter.api.Test;

/**
 * This class contains unit tests for the {@link FirstNameByLastNameAggregator} class.
 */
class FirstNameByLastNameAggregatorTests {
    @Test
    void shouldAggregateFirstNamesByLastName() {
        FirstNameByLastNameAggregator aggregator = new FirstNameByLastNameAggregator();
        KafkaPersonGroup group = new KafkaPersonGroup(new HashMap<>());

        aggregator.apply("Doe", KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName("John")
            .setLastName("Doe")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
            .build(), group);

        aggregator.apply("Doe", KafkaPerson.newBuilder()
            .setId(2L)
            .setFirstName("Michael")
            .setLastName("Doe")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
            .build(), group);

        aggregator.apply("Smith", KafkaPerson.newBuilder()
            .setId(3L)
            .setFirstName("Jane")
            .setLastName("Smith")
            .setBirthDate(Instant.parse("2000-01-01T01:00:00.00Z"))
            .build(), group);

        assertThat(group.getFirstNameByLastName()).containsKey("Doe");
        assertThat(group.getFirstNameByLastName().get("Doe")).hasSize(2);
        assertThat(group.getFirstNameByLastName().get("Doe").get(0)).isEqualTo("John");
        assertThat(group.getFirstNameByLastName().get("Doe").get(1)).isEqualTo("Michael");

        assertThat(group.getFirstNameByLastName()).containsKey("Smith");
        assertThat(group.getFirstNameByLastName().get("Smith")).hasSize(1);
        assertThat(group.getFirstNameByLastName().get("Smith").get(0)).isEqualTo("Jane");
    }
}
