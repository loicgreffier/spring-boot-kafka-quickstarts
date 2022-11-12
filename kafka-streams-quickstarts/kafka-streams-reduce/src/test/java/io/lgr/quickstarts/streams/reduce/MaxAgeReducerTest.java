package io.lgr.quickstarts.streams.reduce;

import io.lgr.quickstarts.avro.KafkaPerson;
import io.lgr.quickstarts.streams.reduce.app.reducer.MaxAgeReducer;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class MaxAgeReducerTest {
    @Test
    void shouldKeepOldestPerson() {
        MaxAgeReducer maxAgeReducer = new MaxAgeReducer();

        KafkaPerson oldest = KafkaPerson.newBuilder()
                .setId(1L)
                .setFirstName("Aaran")
                .setLastName("Abbott")
                .setBirthDate(Instant.parse("1956-08-29T18:35:24.00Z"))
                .build();

        KafkaPerson oldestSameYear = KafkaPerson.newBuilder()
                .setId(2L)
                .setFirstName("Bret")
                .setLastName("Holman")
                .setBirthDate(Instant.parse("1956-02-18T12:00:46.00Z"))
                .build();

        KafkaPerson youngest = KafkaPerson.newBuilder()
                .setId(3L)
                .setFirstName("Brendan")
                .setLastName("Abbott")
                .setBirthDate(Instant.parse("1995-12-15T23:06:22.00Z"))
                .build();

        assertThat(maxAgeReducer.apply(youngest, oldest)).isEqualTo(oldest);
        assertThat(maxAgeReducer.apply(oldest, oldestSameYear)).isEqualTo(oldest);
    }
}
