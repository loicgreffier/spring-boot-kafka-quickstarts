package io.github.loicgreffier.streams.reduce;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.reduce.app.reducer.MaxAgeReducer;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * This class contains unit tests for the {@link MaxAgeReducer} class.
 */
class MaxAgeReducerTests {
    @Test
    void shouldKeepOldestPerson() {
        MaxAgeReducer reducer = new MaxAgeReducer();

        KafkaPerson oldest = KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName("John")
            .setLastName("Doe")
            .setBirthDate(Instant.parse("1956-08-29T18:35:24.00Z"))
            .build();

        KafkaPerson oldestSameYear = KafkaPerson.newBuilder()
            .setId(2L)
            .setFirstName("Jane")
            .setLastName("Smith")
            .setBirthDate(Instant.parse("1956-02-18T12:00:46.00Z"))
            .build();

        KafkaPerson youngest = KafkaPerson.newBuilder()
            .setId(3L)
            .setFirstName("Michael")
            .setLastName("Doe")
            .setBirthDate(Instant.parse("1995-12-15T23:06:22.00Z"))
            .build();

        assertThat(reducer.apply(youngest, oldest)).isEqualTo(oldest);
        assertThat(reducer.apply(oldest, oldestSameYear)).isEqualTo(oldestSameYear);
    }
}
