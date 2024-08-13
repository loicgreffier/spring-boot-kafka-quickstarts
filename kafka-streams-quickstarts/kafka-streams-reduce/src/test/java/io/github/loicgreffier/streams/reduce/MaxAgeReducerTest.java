package io.github.loicgreffier.streams.reduce;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.reduce.app.reducer.MaxAgeReducer;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class MaxAgeReducerTest {
    @Test
    void shouldKeepOldestPerson() {
        MaxAgeReducer reducer = new MaxAgeReducer();

        KafkaPerson oldest = KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName("Homer")
            .setLastName("Simpson")
            .setBirthDate(Instant.parse("1956-08-29T18:35:24Z"))
            .build();

        KafkaPerson oldestSameYear = KafkaPerson.newBuilder()
            .setId(2L)
            .setFirstName("Kirk")
            .setLastName("Van Houten")
            .setBirthDate(Instant.parse("1956-02-18T12:00:46Z"))
            .build();

        KafkaPerson youngest = KafkaPerson.newBuilder()
            .setId(3L)
            .setFirstName("Bart")
            .setLastName("Simpson")
            .setBirthDate(Instant.parse("1995-12-15T23:06:22Z"))
            .build();

        assertEquals(oldest, reducer.apply(youngest, oldest));
        assertEquals(oldestSameYear, reducer.apply(oldest, oldestSameYear));
    }
}
