package io.github.loicgreffier.streams.average;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.loicgreffier.avro.CountryCode;
import io.github.loicgreffier.avro.KafkaAverageAge;
import io.github.loicgreffier.avro.KafkaPerson;
import io.github.loicgreffier.streams.average.app.aggregator.AgeAggregator;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * This class contains unit tests for the {@link AgeAggregator} class.
 */
class AgeAggregatorTest {
    @Test
    void shouldAggregateAgeByNationality() {
        AgeAggregator aggregator = new AgeAggregator();
        KafkaAverageAge averageAge = new KafkaAverageAge(0L, 0L);

        LocalDate currentDate = LocalDate.now();

        KafkaPerson personOne = KafkaPerson.newBuilder()
            .setId(1L)
            .setFirstName("John")
            .setLastName("Doe")
            .setNationality(CountryCode.FR)
            .setBirthDate(
                currentDate.minusYears(25).atStartOfDay().toInstant(ZoneOffset.UTC)) // 25 years old
            .build();
        aggregator.apply(CountryCode.FR.toString(), personOne, averageAge);

        KafkaPerson personTwo = KafkaPerson.newBuilder()
            .setId(2L)
            .setFirstName("Michael")
            .setLastName("Doe")
            .setNationality(CountryCode.FR)
            .setBirthDate(
                currentDate.minusYears(50).atStartOfDay().toInstant(ZoneOffset.UTC)) // 50 years old
            .build();
        aggregator.apply(CountryCode.FR.toString(), personTwo, averageAge);

        KafkaPerson personThree = KafkaPerson.newBuilder()
            .setId(3L)
            .setFirstName("Jane")
            .setLastName("Smith")
            .setNationality(CountryCode.FR)
            .setBirthDate(
                currentDate.minusYears(75).atStartOfDay().toInstant(ZoneOffset.UTC)) // 75 years old
            .build();
        aggregator.apply(CountryCode.FR.toString(), personThree, averageAge);

        assertEquals(3L, averageAge.getCount());
        assertEquals(150L, averageAge.getAgeSum());
    }
}
